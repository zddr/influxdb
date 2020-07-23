package meta

import (
	"context"
	"fmt"
	"time"

	"github.com/influxdata/influxdb/services/meta"
	v2 "github.com/influxdata/influxdb/servicesv2"
)

type Client struct {
	BucketService      v2.BucketService
	DBRPMappingService v2.DBRPMappingServiceV2
	ShardGroupService  v2.ShardGroupService
}

func (c *Client) Database(db string) *meta.DatabaseInfo {
	dbrps, count, err := c.DBRPMappingService.FindMany(
		context.Background(),
		v2.DBRPMappingFilterV2{Database: &db},
	)
	if err != nil {
		return nil
	} else if count < 1 {
		return nil
	}

	dbinfo := meta.DatabaseInfo{
		Name: db,
	}

	for _, mapping := range dbrps {
		rp := mapping.RetentionPolicy
		if mapping.Default {
			dbinfo.DefaultRetentionPolicy = rp
		}

		rpi, err := c.RetentionPolicy(db, rp)
		if err != nil {
			return nil
		}
		dbinfo.RetentionPolicies = append(dbinfo.RetentionPolicies, *rpi)
	}

	return &dbinfo
}

func (c *Client) RetentionPolicy(db, rp string) (*meta.RetentionPolicyInfo, error) {
	dbrps, count, err := c.DBRPMappingService.FindMany(context.Background(), v2.DBRPMappingFilterV2{
		Database:        &db,
		RetentionPolicy: &rp,
	})
	if err != nil {
		return nil, err
	}
	if count != 1 {
		return nil, fmt.Errorf("expected 1 value - got %d", count)
	}

	dbrp := dbrps[0]
	rpi := meta.RetentionPolicyInfo{
		Name:     dbrp.RetentionPolicy,
		ReplicaN: 1,
	}

	bucket, err := c.BucketService.FindBucket(context.Background(), v2.BucketFilter{
		ID: &dbrp.BucketID,
	})
	if err != nil {
		return nil, err
	}
	rpi.Duration = bucket.RetentionPeriod
	rpi.ShardGroupDuration = shardGroupDuration(bucket.RetentionPeriod)
	return &rpi, nil
}

func (c *Client) CreateShardGroup(db, rp string, timestamp time.Time) (*meta.ShardGroupInfo, error) {
	dbrps, count, err := c.DBRPMappingService.FindMany(context.Background(), v2.DBRPMappingFilterV2{
		Database:        &db,
		RetentionPolicy: &rp,
	})
	if err != nil {
		return nil, err
	} else if count != 1 {
		return nil, fmt.Errorf("expected 1 DBRP - got %d", count)
	}
	dbrp := dbrps[0]
	sgi, err := c.ShardGroupService.CreateShardGroup(context.Background(), dbrp.BucketID, timestamp)
	if err != nil {
		return nil, err
	}
	return sgi, nil
}

func shardGroupDuration(d time.Duration) time.Duration {
	if d >= 180*24*time.Hour || d == 0 { // 6 months or 0
		return 7 * 24 * time.Hour
	} else if d >= 2*24*time.Hour { // 2 days
		return 1 * 24 * time.Hour
	}
	return 1 * time.Hour
}
