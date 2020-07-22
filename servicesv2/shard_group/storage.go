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
	kvStore kv.Store
}

func NewStore(kvStore kv.Store) *Store {
	return &Store{
		kvStore: kvStore,
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

		idx, err := tx.Bucket(shardGroupIndex)
		if err != nil {
			return err
		}

		ikey, err := indexKey(bucketID, id)

		return idx.Put(ikey, key)
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

		key, err := id.Encode()
		if err != nil {
			return err
		}
		bytes, err := b.Get(key)
		if err != nil {
			return err
		}

		return json.Unmarshal(bytes, rtn)
	})
	return rtn, err
}

func (s *Store) ListShardGroups(ctx context.Context, filter influxdb.FindShardFilter) ([]meta.ShardGroupInfo, error) {

	sgis := []meta.ShardGroupInfo{}

	keepFn := func(sgi meta.ShardGroupInfo) bool {
		return (filter.Min == nil || sgi.EndTime.After(*filter.Min)) &&
			(filter.Max == nil || !sgi.StartTime.After(*filter.Max)) &&
			(filter.BetweenTime == nil || (sgi.StartTime.Before(*filter.BetweenTime) && sgi.EndTime.After(*filter.BetweenTime)))
	}

	err := s.kvStore.View(ctx, func(tx kv.Tx) error {
		// if bucketID is in the filter lets do a lookup by index
		if filter.BucketID != nil {
			fKey, err := filter.BucketID.Encode()
			if err != nil {
				return err
			}

			idx, err := tx.Bucket(shardGroupIndex)
			if err != nil {
				return err
			}

			cursor, err := idx.ForwardCursor(fKey, kv.WithCursorPrefix(fKey))
			if err != nil {
				return err
			}
			defer cursor.Close()

			for k, v := cursor.Next(); k != nil; k, v = cursor.Next() {
				sgi := meta.ShardGroupInfo{}

				err := json.Unmarshal(v, &sgi)
				if err != nil {
					return err
				}
				if keepFn(sgi) {
					sgis = append(sgis, sgi)
				}

			}
			return cursor.Err()
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
			if keepFn(sgi) {
				sgis = append(sgis, sgi)
			}
		}
		return c.Err()
	})

	if err != nil {
		return nil, err
	}

	return sgis, nil
}

func (s *Store) DeleteShardGroup(ctx context.Context, bucketID, id influxdb.ID) error {
	err := s.kvStore.Update(ctx, func(tx kv.Tx) error {
		b, err := tx.Bucket(shardGroupBucket)
		if err != nil {
			return err
		}
		key, err := id.Encode()
		if err != nil {
			return err
		}

		idx, err := tx.Bucket(shardGroupIndex)
		if err != nil {
			return err
		}

		ikey, err := indexKey(bucketID, id)
		if err != nil {
			return err
		}

		if err := idx.Delete(ikey); err != nil {
			return err
		}

		return b.Delete(key)
	})
	return err
}

func indexKey(b, sg influxdb.ID) ([]byte, error) {
	bucketID, err := b.Encode()

	if err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Err:  err,
		}
	}

	shardGroupID, err := b.Encode()

	if err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Err:  err,
		}
	}

	k := make([]byte, influxdb.IDLength*2)
	copy(k, bucketID)
	copy(k[influxdb.IDLength:], shardGroupID)
	return k, nil
}
