package query

import (
	"context"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/memory"
	"github.com/influxdata/influxdb/flux/builtin"
	iflux "github.com/influxdata/influxdb/flux/stdlib/influxdata/influxdb"
	influxdb "github.com/influxdata/influxdb/servicesv2"
	"github.com/influxdata/influxdb/servicesv2/meta"
)

type QueryService interface {
	Query(ctx context.Context, orgID influxdb.ID, compiler flux.Compiler) (flux.Query, error)
}

type Service struct {
	fluxDeps []flux.Dependency
}

var _ QueryService = (*Service)(nil)

func NewService(metaClient meta.Client, reader iflux.Reader) *Service {
	builtin.Initialize()

	deps, err := iflux.NewDependencies(metaClient, reader, nil, false)
	if err != nil {
		panic(err)
	}

	return &Service{
		fluxDeps: []flux.Dependency{deps},
	}
}

func (s *Service) Query(ctx context.Context, orgID influxdb.ID, compiler flux.Compiler) (flux.Query, error) {
	for _, dep := range s.fluxDeps {
		ctx = dep.Inject(ctx)
	}

	p, err := compiler.Compile(ctx)
	if err != nil {
		return nil, err
	}

	alloc := &memory.Allocator{}
	return p.Start(ctx, alloc)
}
