package query

import (
	"context"

	"github.com/influxdata/flux"
)

type AuthedQueryService struct {
	queryService Service
}

var _ QueryService = (*AuthedQueryService)(nil)

func NewAuthedQueryService(c Service) *AuthedQueryService {
	return &AuthedQueryService{
		queryService: c,
	}
}

func (c *AuthedQueryService) Query(ctx context.Context, compiler flux.Compiler) (flux.Query, error) {
	// authorize the user
	return c.queryService.Query(ctx, compiler)
}
