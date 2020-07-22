package shard_group

import (
	"context"
	"time"

	"github.com/influxdata/influxdb/services/meta"
	influxdb "github.com/influxdata/influxdb/servicesv2"
	"github.com/influxdata/influxdb/servicesv2/snowflake"
)

type Service struct {
	store     *Store
	IDGen     influxdb.IDGenerator
	bucketSvc influxdb.BucketService
}

func NewService(s *Store, bucketSvc influxdb.BucketService) *Service {
	return &Service{
		store:     s,
		IDGen:     snowflake.NewDefaultIDGenerator(),
		bucketSvc: bucketSvc,
	}
}

func (s *Service) CreateShardGroup(ctx context.Context, bucketID influxdb.ID, timestamp time.Time) (*meta.ShardGroupInfo, error) {
	// see if we already have one that timestamp fits into
	sgs, err := s.FindShardGroups(ctx, influxdb.FindShardFilter{
		BucketID:    &bucketID,
		BetweenTime: &timestamp,
	})

	if err != nil {
		return nil, err
	}

	if len(sgs) >= 1 {
		// with the current ugly way to make ShardGroups it is possible to have more then 1
		return &sgs[0], nil
	}

	bucket, err := s.bucketSvc.FindBucketByID(ctx, bucketID)
	if err != nil {
		return nil, err
	}

	// create one that fits within the time constraints of the existing shards or create a new shard group that makes sense for this time stamp
	// TODO: this doesnt attempt to fit the new shard group inside the existing shard group list and can lead to overlaps in shard groups.
	// a better solution is to first check if there is any overlaps and adjust this shard groups start and end time to avoid these overlaps.
	startTime := timestamp.Truncate(bucket.RetentionPeriod).UTC()
	sgi := &meta.ShardGroupInfo{
		ID:        uint64(s.IDGen.ID()),
		StartTime: startTime,
		EndTime:   startTime.Add(bucket.RetentionPeriod).UTC(),
		Shards: []meta.ShardInfo{
			{ID: uint64(s.IDGen.ID())},
		},
	}

	return sgi, s.store.CreateShardGroup(ctx, bucketID, sgi)
}

func (s *Service) FindShardGroups(ctx context.Context, filter influxdb.FindShardFilter) ([]meta.ShardGroupInfo, error) {
	return s.store.ListShardGroups(ctx, filter)
}

func (s *Service) DeleteShardGroup(ctx context.Context, bucketID, id influxdb.ID) error {
	return s.store.DeleteShardGroup(ctx, bucketID, id)
}
