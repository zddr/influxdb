package meta

import (
	"context"
	"fmt"
	"time"

	"github.com/influxdata/influxdb/services/meta"
	influxdb "github.com/influxdata/influxdb/servicesv2"
)

type Client struct {
	BucketService      influxdb.BucketService
	DBRPMappingService influxdb.DBRPMappingServiceV2
	ShardGroupService  influxdb.ShardGroupService
}

func NewClient(bucketSvc influxdb.BucketService, dbrpSvc influxdb.DBRPMappingServiceV2, shardGroupSvc influxdb.ShardGroupService) *Client {
	return &Client{
		BucketService:      bucketSvc,
		DBRPMappingService: dbrpSvc,
		ShardGroupService:  shardGroupSvc,
	}
}

func (c Client) Database(db string) *meta.DatabaseInfo {
	dbrps, count, err := c.DBRPMappingService.FindMany(
		context.Background(),
		influxdb.DBRPMappingFilterV2{Database: &db},
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

func (c Client) RetentionPolicy(db, rp string) (*meta.RetentionPolicyInfo, error) {
	dbrps, count, err := c.DBRPMappingService.FindMany(context.Background(), influxdb.DBRPMappingFilterV2{
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

	bucket, err := c.BucketService.FindBucket(context.Background(), influxdb.BucketFilter{
		ID: &dbrp.BucketID,
	})
	if err != nil {
		return nil, err
	}
	rpi.Duration = bucket.RetentionPeriod
	rpi.ShardGroupDuration = shardGroupDuration(bucket.RetentionPeriod)
	return &rpi, nil
}

func (c Client) CreateShardGroup(db, rp string, timestamp time.Time) (*meta.ShardGroupInfo, error) {
	dbrps, count, err := c.DBRPMappingService.FindMany(context.Background(), influxdb.DBRPMappingFilterV2{
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

func (c Client) ShardGroupsByTimeRange(db, rp string, min, max time.Time) (a []meta.ShardGroupInfo, err error) {
	dbrps, count, err := c.DBRPMappingService.FindMany(context.Background(), influxdb.DBRPMappingFilterV2{
		Database:        &db,
		RetentionPolicy: &rp,
	})
	if err != nil {
		return nil, err
	} else if count != 1 {
		return nil, fmt.Errorf("expected 1 DBRP - got %d", count)
	}
	dbrp := dbrps[0]
	return c.ShardGroupService.FindShardGroups(context.Background(), influxdb.FindShardFilter{
		BucketID: &dbrp.BucketID,
		Min:      &min,
		Max:      &max,
	})
}

func shardGroupDuration(d time.Duration) time.Duration {
	if d >= 180*24*time.Hour || d == 0 { // 6 months or 0
		return 7 * 24 * time.Hour
	} else if d >= 2*24*time.Hour { // 2 days
		return 1 * 24 * time.Hour
	}
	return 1 * time.Hour
}

func (c Client) Databases() []meta.DatabaseInfo {
	return []meta.DatabaseInfo{}
}
