package testing

import influxdb "github.com/influxdata/influxdb/servicesv2"

// IDPtr returns a pointer to an influxdb.ID.
func IDPtr(id influxdb.ID) *influxdb.ID { return &id }
