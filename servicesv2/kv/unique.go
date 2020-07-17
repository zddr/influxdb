package kv

import (
	"context"
	"fmt"

	influxdb "github.com/influxdata/influxdb/servicesv2"
	"github.com/influxdata/influxdb/servicesv2/snowflake"
)

const (
	// MaxIDGenerationN is the maximum number of times an ID generation is done before failing.
	MaxIDGenerationN = 100
	// ReservedIDs are the number of IDs reserved from 1 - ReservedIDs we use
	// for our system org/buckets
	ReservedIDs = 1000
)

// ErrFailureGeneratingID occurs ony when the random number generator
// cannot generate an ID in MaxIDGenerationN times.
var ErrFailureGeneratingID = &influxdb.Error{
	Code: influxdb.EInternal,
	Msg:  "unable to generate valid id",
}

// UnexpectedIndexError is used when the error comes from an internal system.
func UnexpectedIndexError(err error) *influxdb.Error {
	return &influxdb.Error{
		Code: influxdb.EInternal,
		Msg:  fmt.Sprintf("unexpected error retrieving index; Err: %v", err),
		Op:   "kv/index",
	}
}

// NotUniqueError is used when attempting to create a resource that already
// exists.
var NotUniqueError = &influxdb.Error{
	Code: influxdb.EConflict,
	Msg:  "name already exists",
}

// NotUniqueIDError is used when attempting to create an org or bucket that already
// exists.
var NotUniqueIDError = &influxdb.Error{
	Code: influxdb.EConflict,
	Msg:  "ID already exists",
}

func unique(ctx context.Context, tx Tx, indexBucket, indexKey []byte) error {
	bucket, err := tx.Bucket(indexBucket)
	if err != nil {
		return UnexpectedIndexError(err)
	}

	_, err = bucket.Get(indexKey)
	// if not found then this is  _unique_.
	if IsNotFound(err) {
		return nil
	}

	// no error means this is not unique
	if err == nil {
		return NotUniqueError
	}

	// any other error is some sort of internal server error
	return UnexpectedIndexError(err)
}

func uniqueID(ctx context.Context, tx Tx, bucket []byte, id influxdb.ID) error {
	encodedID, err := id.Encode()
	if err != nil {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Err:  err,
		}
	}

	b, err := tx.Bucket(bucket)
	if err != nil {
		return err
	}

	_, err = b.Get(encodedID)
	if IsNotFound(err) {
		return nil
	}

	return NotUniqueIDError
}

// generateSafeID attempts to create ids for buckets
// and orgs that are without backslash, commas, and spaces, BUT ALSO do not already exist.
func generateSafeID(ctx context.Context, tx Tx, bucket []byte) (influxdb.ID, error) {
	for i := 0; i < MaxIDGenerationN; i++ {
		id := snowflake.NewIDGenerator().ID()
		// we have reserved a certain number of IDs
		// for orgs and buckets.
		if id < ReservedIDs {
			continue
		}
		err := uniqueID(ctx, tx, bucket, id)
		if err == nil {
			return id, nil
		}

		if err == NotUniqueIDError {
			continue
		}

		return influxdb.InvalidID(), err
	}
	return influxdb.InvalidID(), ErrFailureGeneratingID
}
