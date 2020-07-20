package influxdb

import (
	"context"
	"time"

	"github.com/influxdata/influxdb/services/meta"
)

type FindShardFilter struct {
	BucketID    *ID
	Min         *time.Time
	Max         *time.Time
	BetweenTime *time.Time // allows us to find a shard where shard.StartTime < betweenTime < shard.EndTme
}

type ShardGroupService interface {
	CreateShardGroup(ctx context.Context, bucketID ID, timestamp time.Time) (*meta.ShardGroupInfo, error)

	FindShardGroups(ctx context.Context, filter FindShardFilter) ([]meta.ShardGroupInfo, error)

	DeleteShardGroup(ctx context.Context, id uint64) error
}
