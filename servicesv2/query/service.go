package query

import (
	"context"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/memory"
	"github.com/influxdata/influxdb/flux/builtin"
)

type QueryService interface {
	Query(ctx context.Context, compiler flux.Compiler) (flux.Query, error)
}

type Service struct {
	fluxDeps []flux.Dependency
}

var _ QueryService = (*Service)(nil)

func NewService() *Service {
	builtin.Initialize()

	deps := flux.NewDefaultDependencies()
	// if err != nil {
	// 	panic(err)
	// }

	return &Service{
		fluxDeps: []flux.Dependency{deps},
	}
}

func (s *Service) Query(ctx context.Context, compiler flux.Compiler) (flux.Query, error) {
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
