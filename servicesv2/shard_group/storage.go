package shard_group

import (
	"context"
	"encoding/json"

	"github.com/influxdata/influxdb/services/meta"
	influxdb "github.com/influxdata/influxdb/servicesv2"
	"github.com/influxdata/influxdb/servicesv2/kv"
)

var (
	shardGroupBucket = []byte("shardgroupsv1")

	// shardGroupIndex allows us to lookup shard groups by bucketID
	shardGroupIndex = []byte("shardgroupindexv1")
)

type Store struct {
	kvStore         kv.Store
	shardGroupIndex *kv.Index
}

func NewStore(kvStore kv.Store) *Store {
	return &Store{
		kvStore:         kvStore,
		shardGroupIndex: kv.NewIndex(shardGroupIndex, kv.WithIndexReadPathEnabled),
	}
}

func (s *Store) CreateShardGroup(ctx context.Context, bucketID influxdb.ID, sg *meta.ShardGroupInfo) error {
	err := s.kvStore.Update(ctx, func(tx kv.Tx) error {
		b, err := tx.Bucket(shardGroupBucket)
		if err != nil {
			return err
		}

		id := influxdb.ID(sg.ID)
		key, err := id.Encode()
		if err != nil {
			return err
		}
		val, err := json.Marshal(sg)
		if err != nil {
			return err
		}

		if err := b.Put(key, val); err != nil {
			return err
		}

		bucketKey, err := bucketID.Encode()
		if err != nil {
			return err
		}

		return s.shardGroupIndex.Insert(tx, bucketKey, key)
	})

	return err
}

func (s *Store) FindShardGroup(ctx context.Context, id influxdb.ID) (*meta.ShardGroupInfo, error) {
	var rtn *meta.ShardGroupInfo
	err := s.kvStore.View(ctx, func(tx kv.Tx) error {
		b, err := tx.Bucket(shardGroupBucket)
		if err != nil {
			return err
		}

		id := influxdb.ID(sg.ID)
		key, err := id.Encode()
		if err != nil {
			return err
		}
		bytes, err := b.Get(key)
		if err != nil {
			return err
		}

		return json.Unmarshal(bytest, rtn)
	})
	return rtn, err
}

func (s *Store) ListShardGroups(ctx context.Context, filter influxdb.FindShardFilter) ([]meta.ShardGroupInfo, error) {

	sgis := []meta.ShardGroupInfo{}

	filterFn := func(sgi meta.ShardGroupInfo) bool {
		return (filter.Min == nil || (sgi.EndTime.After(*filter.Min)) &&
		(filter.Max == nil || (!sgi.StartTime.After(*filter.Max)) &&
		(filter.BetweenTime == nil || (sgi.StartTime.Before(*filter.BetweenTime) && shard.EndTme.After(*filter.BetweenTime))
	}

	err := s.kvStore.View(ctx, func(tx kv.Tx) error {
		// if bucketID is in the filter lets do a lookup by index
		if filter.BucketID != nil {
			fKey, err := filter.BucketID.Encode()
			if err != nil {
				return err
			}

			err =s.shardGroupIndex.Walk(ctx, tx, fKey, func(k, v []byte) error {
				sgi := meta.ShardGroupInfo{}

				err := json.Unmarshal(v, &sgi)
				if err != nil {
					return err
				}
				if filterFn(sgi) {
					sgis = append(sgis, sgi)
				}

			})
			return err
		}

		b, err := tx.Bucket(shardGroupBucket)
		if err != nil {
			return err
		}

		c, err := b.ForwardCursor(nil)
		if err != nil {
			return err
		}
		defer c.Close()

		for k, v := c.Next(); k != nil; k, v = c.Next() {
			sgi := meta.ShardGroupInfo{}

			err := json.Unmarshal(v, &sgi)
			if err != nil {
				return err
			}
			if filterFn(sgi) {
				sgis = append(sgis, sgi)
			}
		}
		return c.Err()
	}

	if err != nil {
		return nil, err
	}

	return sgis, nil
}

func (s *Store) DeleteShardGroup(ctx context.Context, bucketID, id influxdb.ID) error {
	key, err := id.Encode()
	if err != nil {
		return err
	}

	foreignKey, err := bucketID.Encode()
	if err != nil {
		return err
	}

	err = s.kvStore.Update(ctx, func(tx kv.Tx) error {
		if err :=s.shardGroupIndex.Delete(tx, foreignKey, key); err != nil {
			return err
		}

		b, err := tx.Bucket(shardGroupBucket)
		if err != nil {
			return err
		}
		key, err := id.Encode()
		if err != nil {
			return err
		}
		return b.Delete(key)
	})
	return err
}
