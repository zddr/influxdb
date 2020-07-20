package shard_group

import (
	"context"
	"time"

	"github.com/influxdata/influxdb/services/meta"
	influxdb "github.com/influxdata/influxdb/servicesv2"
	"github.com/influxdata/influxdb/servicesv2/snowflake"

)

type Service struct {
	store *Store
	IDGen          influxdb.IDGenerator

}

func NewService(s *Store) *Service {
	return &Service{
		store: s,
		IDGen: snowflake.NewDefaultIDGenerator(),
	}
}

func (s *Service) CreateShardGroup(ctx context.Context, bucketID influxdb.ID, timestamp time.Time) (*meta.ShardGroupInfo, error) {
	// see if we already have one that timestamp fits into
	sgs, err := s.FindShardGroups(ctx, influxdb.FindShardFilter{
		BucketID: &bucketID,
		BetweenTime: &timestamp,
	})

	if err != nil {
		return nil, err
	}

	if len(sgs) >= 1 {
		// with the current ugly way to make ShardGroups it is possible to have more then 1
		return sgs[0], nil
	}

	// create one that fits within the time constraints of the existing shards or create a new shard group that makes sense for this time stamp
	// TODO: this doesnt attempt to fit the new shard group inside the existing shard group list and can lead to overlaps in shard groups.
	// a better solution is to first check if there is any overlaps and adjust this shard groups start and end time to avoid these overlaps. 
	sgi := &meta.ShardGroupInfo{
		ID: uint64(s.IDGen.ID())
		StartTime: timestamp.Truncate(rpi.ShardGroupDuration).UTC(),
		EndTime: sgi.StartTime.Add(rpi.ShardGroupDuration).UTC(),
		Shards: []meta.ShardInfo{
			{ID: uint64(s.IDGen.ID())},
		},
	}

	return sgi, s.store.CreateShardGroup(ctx, bucketID, sgi)
}

func (s *Service) FindShardGroups(ctx context.Context, filter influxdb.FindShardFilter) ([]meta.ShardGroupInfo, error) {
	// lookup the bucket based on database (which is the bucket name) how do we know what org this is?
	// create a filter that includes the times and the bucket id to make the lookup quick
	// return shard groups
}

func (s *Service) DeleteShardGroup(ctx context.Context, id influxdb.ID) error {
	// delete the shard that belongs to this bucket. ID's should be unique so we should be able to just delete this
}
