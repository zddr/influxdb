package tenant_test

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi"
	influxdb "github.com/influxdata/influxdb/servicesv2"
	ihttp "github.com/influxdata/influxdb/servicesv2/api"
	"github.com/influxdata/influxdb/servicesv2/tenant"
	influxdbtesting "github.com/influxdata/influxdb/servicesv2/testing"
	"go.uber.org/zap/zaptest"
)

func initHttpUserService(f influxdbtesting.UserFields, t *testing.T) (influxdb.UserService, string, func()) {
	t.Helper()

	s, stCloser, err := tenant.NewTestBoltStore(t)
	if err != nil {
		t.Fatal(err)
	}

	storage := tenant.NewStore(s)
	svc := tenant.NewService(storage)

	ctx := context.Background()
	for _, u := range f.Users {
		if err := svc.CreateUser(ctx, u); err != nil {
			t.Fatalf("failed to populate users")
		}
	}

	handler := tenant.NewHTTPUserHandler(zaptest.NewLogger(t), svc, svc)
	r := chi.NewRouter()
	r.Mount("/api/v2/users", handler)
	r.Mount("/api/v2/me", handler)
	server := httptest.NewServer(r)

	httpClient, err := ihttp.NewHTTPClient(server.URL, "", false)
	if err != nil {
		t.Fatal(err)
	}

	client := tenant.UserClientService{
		Client: httpClient,
	}

	return &client, "http_tenant", func() {
		server.Close()
		stCloser()
	}
}

func TestUserService(t *testing.T) {
	t.Parallel()
	influxdbtesting.UserService(initHttpUserService, t)
}
