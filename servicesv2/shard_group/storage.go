package shard_group

import (
	"context"

	influxdb "github.com/influxdata/influxdb/servicesv2"
	"github.com/influxdata/influxdb/servicesv2/kv"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/servicesv2/snowflake"
)

var (
	shardGroupBucket = []byte("shardgroupsv1")

	// shardGroupIndex allows us to lookup shard groups by bucketID
	shardGroupIndex  = []byte("shardgroupindexv1")
)

type shardGroupFilter struct {
	bucketID *influxdb.ID
	startTime *time.Time
	endTime *time.Time
	shardID *influxdb.ID
}

type Store struct {
	kvStore        kv.Store
	IDGen          influxdb.IDGenerator
	shardGroupIndex *kv.Index
}

func NewStore(kvStore kv.Store) *Store {
	return &Store{
		kvStore:        kvStore,
		IDGen:          snowflake.NewDefaultIDGenerator(),
		shardGroupIndex: kv.NewIndex(shardGroupIndex, kv.WithIndexReadPathEnabled),
	}
}

func (s *Store) CreateShardGroup(ctx context.Context, bucketID influxdb.ID, *meta.ShardGroupInfo) error {

}

func (s *Store) FindShardGroup(ctx context.Context, id influxdb.ID) (*meta.ShardGroupInfo, error) {

}

func (s *Store) ListShardGroups(ctx context.Context, filter shardGroupFilter) ([]*meta.ShardGroupInfo, error) {

}

func (s *Store) DeleteShardGroup(ctx context.Context, id influxdb.ID) error {

}
