package query_test

import (
	"context"
	"testing"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/memory"
	"github.com/influxdata/influxdb/servicesv2/query"
	"github.com/prometheus/client_golang/prometheus"
)

func TestServer_HandleQuery(t *testing.T) {
	h := query.NewQueryHandler()
}

// ****** MOCKS ****** //
type FluxControllerMock struct {
	QueryFn func(ctx context.Context, compiler flux.Compiler) (flux.Query, error)
}

func NewFluxControllerMock() *FluxControllerMock {
	return &FluxControllerMock{
		QueryFn: func(ctx context.Context, compiler flux.Compiler) (query flux.Query, e error) {
			p, err := compiler.Compile(ctx)
			if err != nil {
				return nil, err
			}
			alloc := &memory.Allocator{}
			return p.Start(ctx, alloc)
		},
	}
}

func (m *FluxControllerMock) Query(ctx context.Context, compiler flux.Compiler) (flux.Query, error) {
	return m.QueryFn(ctx, compiler)
}

func (m *FluxControllerMock) PrometheusCollectors() []prometheus.Collector { return nil }
