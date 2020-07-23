package query

import (
	"context"

	"github.com/influxdata/flux"
	influxdb "github.com/influxdata/influxdb/servicesv2"
	"github.com/influxdata/influxdb/servicesv2/authorizer"
)

type AuthedQueryService struct {
	queryService QueryService
}

var _ QueryService = (*AuthedQueryService)(nil)

func NewAuthedQueryService(s QueryService) *AuthedQueryService {
	return &AuthedQueryService{
		queryService: s,
	}
}

func (c *AuthedQueryService) Query(ctx context.Context, orgID influxdb.ID, compiler flux.Compiler) (flux.Query, error) {
	if _, _, err := authorizer.AuthorizeReadOrg(ctx, orgID); err != nil {
		return nil, err
	}

	return c.queryService.Query(ctx, orgID, compiler)
}
