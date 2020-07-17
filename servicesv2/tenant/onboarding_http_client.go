package tenant

import (
	"context"
	"fmt"
	"path"

	influxdb "github.com/influxdata/influxdb/servicesv2"
	"github.com/influxdata/influxdb/servicesv2/httpc"
)

// OnboardClientService connects to Influx via HTTP to perform onboarding operations
type OnboardClientService struct {
	Client *httpc.Client
}

// IsOnboarding determine if onboarding request is allowed.
func (s *OnboardClientService) IsOnboarding(ctx context.Context) (bool, error) {
	var resp isOnboardingResponse
	err := s.Client.
		Get(prefixOnboard).
		DecodeJSON(&resp).
		Do(ctx)

	if err != nil {
		return false, err
	}
	return resp.Allowed, nil
}

// OnboardInitialUser OnboardingResults.
func (s *OnboardClientService) OnboardInitialUser(ctx context.Context, or *influxdb.OnboardingRequest) (*influxdb.OnboardingResults, error) {
	res := &onboardingResponse{}

	err := s.Client.
		PostJSON(or, prefixOnboard).
		DecodeJSON(res).
		Do(ctx)

	if err != nil {
		fmt.Println("Err: ", err)
		return nil, err
	}

	bkt, err := res.Bucket.toInfluxDB()
	if err != nil {
		return nil, err
	}

	return &influxdb.OnboardingResults{
		Org:    &res.Organization.Organization,
		User:   &res.User.User,
		Auth:   res.Auth.toPlatform(),
		Bucket: bkt,
	}, nil
}

func (s *OnboardClientService) OnboardUser(ctx context.Context, or *influxdb.OnboardingRequest) (*influxdb.OnboardingResults, error) {
	res := &onboardingResponse{}

	err := s.Client.
		PostJSON(or, path.Join(prefixOnboard, "user")).
		DecodeJSON(res).
		Do(ctx)

	if err != nil {
		return nil, err
	}

	bkt, err := res.Bucket.toInfluxDB()
	if err != nil {
		return nil, err
	}

	return &influxdb.OnboardingResults{
		Org:    &res.Organization.Organization,
		User:   &res.User.User,
		Auth:   res.Auth.toPlatform(),
		Bucket: bkt,
	}, nil
}
