package bolt

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	influxdb "github.com/influxdata/influxdb/servicesv2"
	"github.com/influxdata/influxdb/servicesv2/rand"
	"github.com/influxdata/influxdb/servicesv2/snowflake"
	bolt "go.etcd.io/bbolt"
	"go.uber.org/zap"
)

var (
	authorizationBucket = []byte("authorizationsv1")
	authIndex           = []byte("authorizationindexv1")
	bucketBucket        = []byte("bucketsv1")
	organizationBucket  = []byte("organizationsv1")
	userBucket          = []byte("usersv1")
	dbrpBucket          = []byte("dbrpv1")
	dbrpIndexBucket     = []byte("dbrpbyorganddbindexv1")
	dbrpDefaultBucket   = []byte("dbrpdefaultv1")
)

const DefaultFilename = "influxd.bolt"

// Client is a client for the boltDB data store.
type Client struct {
	Path string
	db   *bolt.DB
	log  *zap.Logger

	IDGenerator    influxdb.IDGenerator
	TokenGenerator influxdb.TokenGenerator
	influxdb.TimeGenerator
}

// NewClient returns an instance of a Client.
func NewClient(log *zap.Logger) *Client {
	return &Client{
		log:            log,
		IDGenerator:    snowflake.NewIDGenerator(),
		TokenGenerator: rand.NewTokenGenerator(64),
		TimeGenerator:  influxdb.RealTimeGenerator{},
	}
}

// DB returns the clients DB.
func (c *Client) DB() *bolt.DB {
	return c.db
}

// Open / create boltDB file.
func (c *Client) Open() error {
	// Ensure the required directory structure exists.
	if err := os.MkdirAll(filepath.Dir(c.Path), 0700); err != nil {
		return fmt.Errorf("unable to create directory %s: %v", c.Path, err)
	}

	if _, err := os.Stat(c.Path); err != nil && !os.IsNotExist(err) {
		return err
	}

	// Open database file.
	db, err := bolt.Open(c.Path, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return fmt.Errorf("unable to open boltdb; is there a chronograf already running?  %v", err)
	}
	c.db = db

	if err := c.initialize(); err != nil {
		return err
	}

	c.log.Info("Resources opened", zap.String("path", c.Path))
	return nil
}

// initialize creates Buckets that are missing
func (c *Client) initialize() error {
	if err := c.db.Update(func(tx *bolt.Tx) error {
		// Always create ID bucket.
		// TODO: is this still needed?
		if err := c.initializeID(tx); err != nil {
			return err
		}

		// TODO: make card to normalize everything under kv?
		bkts := [][]byte{
			authorizationBucket,
			authIndex,
			bucketBucket, //bolt bucket of TS buckets
			organizationBucket,
			userBucket,
			dbrpBucket,
			dbrpIndexBucket,
			dbrpDefaultBucket,
		}
		for _, bktName := range bkts {
			if _, err := tx.CreateBucketIfNotExists(bktName); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}

	return nil
}

// Close the connection to the bolt database
func (c *Client) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}
