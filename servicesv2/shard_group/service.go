package shard_group

import (
	"time"

	"github.com/influxdata/influxdb/services/meta"
)

type Service struct {
	store *Store
}

func NewService(s *Store) *Service {
	return &Service{s}
}

func (s *Service) CreateShardGroup(database, policy string, timestamp time.Time) (*meta.ShardGroupInfo, error) {
	// find shard groups by database
	// see if we already have one that timestamp fits into
	// create one that fits within the time constraints of the existing shards or create a new shard group that makes sense for this time stamp

	// return new or existing created shard
}

func (s *Service) ShardGroupsByTimeRange(database, policy string, min, max time.Time) ([]meta.ShardGroupInfo, error) {
	// lookup the bucket based on database (which is the bucket name) how do we know what org this is?
	// create a filter that includes the times and the bucket id to make the lookup quick
	// return shard groups
}

func (s *Service) DeleteShardGroup(database string, policy string, id uint64) error {
	// delete the shard that belongs to this bucket. ID's should be unique so we should be able to just delete this
}
