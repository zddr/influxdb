package query

import (
	"context"

	"github.com/influxdata/flux"
	"github.com/influxdata/influxdb/services/httpd"
	"github.com/prometheus/client_golang/prometheus"
)

type AuthedFluxController struct {
	fluxController httpd.Controller
}

var _ httpd.Controller = (*AuthedFluxController)(nil)

func NewAuthedFluxController(c httpd.Controller) *AuthedFluxController {
	return &AuthedFluxController{
		fluxController: c,
	}
}

func (c *AuthedFluxController) Query(ctx context.Context, compiler flux.Compiler) (flux.Query, error) {
	// authorize the user
	return c.fluxController.Query(ctx, compiler)
}

func (c *AuthedFluxController) PrometheusCollectors() []prometheus.Collector {
	return []prometheus.Collector{}
}
