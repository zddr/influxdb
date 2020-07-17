package tenant_test

import (
	"context"
	"testing"

	influxdb "github.com/influxdata/influxdb/servicesv2"
	"github.com/influxdata/influxdb/servicesv2/authorization"
	"github.com/influxdata/influxdb/servicesv2/kv"
	"github.com/influxdata/influxdb/servicesv2/tenant"
	influxdbtesting "github.com/influxdata/influxdb/servicesv2/testing"
)

func TestBoltOnboardingService(t *testing.T) {
	influxdbtesting.OnboardInitialUser(initBoltOnboardingService, t)
}

func initBoltOnboardingService(f influxdbtesting.OnboardingFields, t *testing.T) (influxdb.OnboardingService, func()) {
	s, closeStore, err := tenant.NewTestBoltStore(t)
	if err != nil {
		t.Fatalf("failed to create new bolt kv store: %v", err)
	}

	svc := initOnboardingService(s, f, t)
	return svc, func() {
		closeStore()
	}
}

func initOnboardingService(s kv.Store, f influxdbtesting.OnboardingFields, t *testing.T) influxdb.OnboardingService {
	storage := tenant.NewStore(s)
	ten := tenant.NewService(storage)

	// we will need an auth service as well
	mockAuth := authorization.NewMockAuthorizationService()
	svc := tenant.NewOnboardService(storage, mockAuth)

	ctx := context.Background()

	t.Logf("Onboarding: %v", f.IsOnboarding)
	if !f.IsOnboarding {
		// create a dummy so so we can no longer onboard
		err := ten.CreateUser(ctx, &influxdb.User{Name: "dummy", Status: influxdb.Active})
		if err != nil {
			t.Fatal(err)
		}
	}

	return svc
}
