package influxdb

import (
	"context"
	"time"

	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/v2"
)

type FindShardFilter struct {
	BucketID    *influxdb.ID
	Min         *time.Time
	Max         *time.Time
	BetweenTime *time.Time // allows us to find a shard where shard.StartTime < betweenTime < shard.EndTme
}

type ShardGroupService interface {
	CreateShardGroup(ctx context.Context, bucketID influxdb.ID, timestamp time.Time) (*meta.ShardGroupInfo, error)

	FindShardGroups(ctx context.Context, filter FindShardFilter) ([]meta.ShardGroupInfo, error)

	DeleteShardGroup(ctx context.Context, id uint64) error
}
