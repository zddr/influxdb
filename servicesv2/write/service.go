package write

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/services/meta"
	influxdb "github.com/influxdata/influxdb/servicesv2"
	"github.com/influxdata/influxdb/tsdb"
)

// Service
type Service struct {
	bucketSvc     influxdb.BucketService
	shardGroupSvc influxdb.ShardGroupService

	store *tsdb.Store
}

func NewService(store *tsdb.Store, bucketSvc influxdb.BucketService, shardGroupSvc influxdb.ShardGroupService) *Service {
	return &Service{
		bucketSvc:     bucketSvc,
		shardGroupSvc: shardGroupSvc,
		store:         store,
	}
}

func (s *Service) WritePoints(ctx context.Context, orgID, bucketID influxdb.ID, points []models.Point) error {
	bucket, err := s.bucketSvc.FindBucketByID(ctx, bucketID)
	if err != nil {
		return err
	}

	// map the points
	mappedPoints, err := s.mapPoints(ctx, bucket, points)
	if err != nil {
		return err
	}

	// tell the store about this stuff :)
	ch := make(chan error, len(mappedPoints))
	wg := sync.WaitGroup{}
	wg.Add(len(mappedPoints))
	for shardID, points := range mappedPoints {
		go func(shardID uint64, bucket *influxdb.Bucket, points []models.Point) {
			err := s.writeToShard(ctx, shardID, bucket, points)
			if err != nil {
				ch <- err
			}
			wg.Done()
		}(shardID, bucket, points)
	}

	wg.Wait()

	select {
	case err := <-ch:
		return err
	default:
	}

	return nil
}

func (s *Service) mapPoints(ctx context.Context, bucket *influxdb.Bucket, points []models.Point) (map[uint64][]models.Point, error) {

	// Holds all the shard groups and shards that are required for writes.
	min := time.Unix(0, models.MinNanoTime)
	if bucket.RetentionPeriod > 0 {
		min = time.Now().Add(-bucket.RetentionPeriod)
	}

	list := make(sgList, 0, 8)
	for _, p := range points {
		// Either the point is outside the scope of the RP, or we already have
		// a suitable shard group for the point.
		if p.Time().Before(min) || list.Covers(p.Time()) {
			continue
		}

		// No shard groups overlap with the point's time, so we will create
		// a new shard group for this point.
		sg, err := s.shardGroupSvc.CreateShardGroup(ctx, bucket.ID, p.Time())
		if err != nil {
			return nil, err
		}

		if sg == nil {
			return nil, errors.New("nil shard group")
		}
		list = list.Append(*sg)
	}

	pointMap := map[uint64][]models.Point{}
	for _, p := range points {
		sg := list.ShardGroupAt(p.Time())
		if sg != nil {
			sh := sg.ShardFor(p.HashID())
			pointMap[sh.ID] = append(pointMap[sh.ID], p)
		}
	}
	return pointMap, nil
}

// writeToShards writes points to a shard.
func (s *Service) writeToShard(ctx context.Context, shardID uint64, b *influxdb.Bucket, points []models.Point) error {

	err := s.store.WriteToShard(shardID, points)
	if err == nil {
		return nil
	}

	// Except tsdb.ErrShardNotFound no error can be handled here
	if err != tsdb.ErrShardNotFound {
		return err
	}

	// If we've written to shard that should exist on the current node, but the store has
	// not actually created this shard, tell it to create it and retry the write
	if err = s.store.CreateShard(b.Name, b.RetentionPolicyName, shardID, true); err != nil {
		return err
	}

	if err = s.store.WriteToShard(shardID, points); err != nil {
		return err
	}

	return nil
}

type sgList meta.ShardGroupInfos

func (l sgList) Covers(t time.Time) bool {
	if len(l) == 0 {
		return false
	}
	return l.ShardGroupAt(t) != nil
}

func (l sgList) ShardGroupAt(t time.Time) *meta.ShardGroupInfo {
	idx := sort.Search(len(l), func(i int) bool { return l[i].EndTime.After(t) })

	// We couldn't find a shard group the point falls into.
	if idx == len(l) || t.Before(l[idx].StartTime) {
		return nil
	}
	return &l[idx]
}

func (l sgList) Append(sgi meta.ShardGroupInfo) sgList {
	next := append(l, sgi)
	sort.Sort(meta.ShardGroupInfos(next))
	return next
}
