package query_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/mock"
	"github.com/influxdata/influxdb/flux/client"
	"github.com/influxdata/influxdb/servicesv2/query"
	"github.com/prometheus/client_golang/prometheus"
)

func TestServer_HandleQuery(t *testing.T) {
	h := query.NewHTTPQueryHandler(NewQueryServiceMock())
	qry := "foo"

	q := client.QueryRequest{Query: qry}
	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(q); err != nil {
		t.Fatalf("unexpected JSON encoding error: %q", err.Error())
	}

	req := MustNewRequest("POST", "/api/v2/query", &body)
	req.Header.Add("content-type", "application/json")

	w := httptest.NewRecorder()

	h.HandleQuery(w, req)

	if got := w.Code; !cmp.Equal(got, http.StatusOK) {
		t.Fatalf("unexpected status: %d", got)
	}

}

// ****** MOCKS ****** //
type QueryServiceMock struct {
	QueryFn func(ctx context.Context, compiler flux.Compiler) (flux.Query, error)
}

func NewQueryServiceMock() *QueryServiceMock {
	return &QueryServiceMock{
		QueryFn: func(ctx context.Context, compiler flux.Compiler) (query flux.Query, e error) {
			p := &mock.Program{}
			return p.Start(ctx, nil)
		},
	}
}

func (m *QueryServiceMock) Query(ctx context.Context, compiler flux.Compiler) (flux.Query, error) {
	return m.QueryFn(ctx, compiler)
}

func (m *QueryServiceMock) PrometheusCollectors() []prometheus.Collector { return nil }

// MustNewRequest returns a new HTTP request. Panic on error.
func MustNewRequest(method, urlStr string, body io.Reader) *http.Request {
	r, err := http.NewRequest(method, urlStr, body)
	if err != nil {
		panic(err.Error())
	}
	return r
}
