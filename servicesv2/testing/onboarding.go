package testing

import (
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	influxdb "github.com/influxdata/influxdb/servicesv2"
)

var onboardCmpOptions = cmp.Options{
	cmp.Comparer(func(x, y *influxdb.OnboardingResults) bool {
		if x == nil && y == nil {
			return true
		}
		if x != nil && y == nil || y != nil && x == nil {
			return false
		}

		return x.User.Name == y.User.Name && x.User.OAuthID == y.User.OAuthID && x.User.Status == y.User.Status &&
			x.Org.Name == y.Org.Name && x.Org.Description == y.Org.Description &&
			x.Bucket.Type == y.Bucket.Type && x.Bucket.Description == y.Bucket.Description && x.Bucket.RetentionPolicyName == y.Bucket.RetentionPolicyName && x.Bucket.RetentionPeriod == y.Bucket.RetentionPeriod && x.Bucket.Name == y.Bucket.Name &&
			(x.Auth != nil && y.Auth != nil && cmp.Equal(x.Auth.Permissions, y.Auth.Permissions)) // its possible auth wont exist on the basic service level
	}),
}

// OnboardingFields will include the IDGenerator, TokenGenerator
// and IsOnboarding
type OnboardingFields struct {
	IDGenerator    influxdb.IDGenerator
	TokenGenerator influxdb.TokenGenerator
	TimeGenerator  influxdb.TimeGenerator
	IsOnboarding   bool
}

// OnboardInitialUser testing
func OnboardInitialUser(
	init func(OnboardingFields, *testing.T) (influxdb.OnboardingService, func()),
	t *testing.T,
) {
	type args struct {
		request *influxdb.OnboardingRequest
	}
	type wants struct {
		errCode string
		results *influxdb.OnboardingResults
	}
	tests := []struct {
		name   string
		fields OnboardingFields
		args   args
		wants  wants
	}{
		{
			name: "denied",
			fields: OnboardingFields{
				IDGenerator: &loopIDGenerator{
					s: []string{oneID, twoID, threeID, fourID},
				},
				TokenGenerator: NewTokenGenerator(oneToken, nil),
				IsOnboarding:   false,
			},
			wants: wants{
				errCode: influxdb.EConflict,
			},
		},
		{
			name: "missing password",
			fields: OnboardingFields{
				IDGenerator: &loopIDGenerator{
					s: []string{oneID, twoID, threeID, fourID},
				},
				TokenGenerator: NewTokenGenerator(oneToken, nil),
				IsOnboarding:   true,
			},
			args: args{
				request: &influxdb.OnboardingRequest{
					User:   "admin",
					Org:    "org1",
					Bucket: "bucket1",
				},
			},
			wants: wants{
				errCode: influxdb.EEmptyValue,
			},
		},
		{
			name: "missing username",
			fields: OnboardingFields{
				IDGenerator: &loopIDGenerator{
					s: []string{oneID, twoID, threeID, fourID},
				},
				TokenGenerator: NewTokenGenerator(oneToken, nil),
				IsOnboarding:   true,
			},
			args: args{
				request: &influxdb.OnboardingRequest{
					Org:    "org1",
					Bucket: "bucket1",
				},
			},
			wants: wants{
				errCode: influxdb.EEmptyValue,
			},
		},
		{
			name: "missing org",
			fields: OnboardingFields{
				IDGenerator: &loopIDGenerator{
					s: []string{oneID, twoID, threeID, fourID},
				},
				TokenGenerator: NewTokenGenerator(oneToken, nil),
				IsOnboarding:   true,
			},
			args: args{
				request: &influxdb.OnboardingRequest{
					User:   "admin",
					Bucket: "bucket1",
				},
			},
			wants: wants{
				errCode: influxdb.EEmptyValue,
			},
		},
		{
			name: "missing bucket",
			fields: OnboardingFields{
				IDGenerator: &loopIDGenerator{
					s: []string{oneID, twoID, threeID, fourID},
				},
				TokenGenerator: NewTokenGenerator(oneToken, nil),
				IsOnboarding:   true,
			},
			args: args{
				request: &influxdb.OnboardingRequest{
					User: "admin",
					Org:  "org1",
				},
			},
			wants: wants{
				errCode: influxdb.EEmptyValue,
			},
		},
		{
			name: "valid onboarding json should create a user, org, bucket, and authorization",
			fields: OnboardingFields{
				IDGenerator: &loopIDGenerator{
					s: []string{oneID, twoID, threeID, fourID},
				},
				TimeGenerator:  TimeGenerator{FakeValue: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC)},
				TokenGenerator: NewTokenGenerator(oneToken, nil),
				IsOnboarding:   true,
			},
			args: args{
				request: &influxdb.OnboardingRequest{
					User:            "admin",
					Org:             "org1",
					Bucket:          "bucket1",
					Password:        "password1",
					RetentionPeriod: 24 * 7, // 1 week
				},
			},
			wants: wants{
				results: &influxdb.OnboardingResults{
					User: &influxdb.User{
						ID:     MustIDBase16(oneID),
						Name:   "admin",
						Status: influxdb.Active,
					},
					Org: &influxdb.Organization{
						ID:   MustIDBase16(twoID),
						Name: "org1",
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC),
							UpdatedAt: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC),
						},
					},
					Bucket: &influxdb.Bucket{
						ID:              MustIDBase16(threeID),
						Name:            "bucket1",
						OrgID:           MustIDBase16(twoID),
						RetentionPeriod: time.Hour * 24 * 7,
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC),
							UpdatedAt: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC),
						},
					},
					Auth: &influxdb.Authorization{
						ID:          MustIDBase16(fourID),
						Token:       oneToken,
						Status:      influxdb.Active,
						UserID:      MustIDBase16(oneID),
						Description: "admin's Token",
						OrgID:       MustIDBase16(twoID),
						Permissions: influxdb.OperPermissions(),
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC),
							UpdatedAt: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC),
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()
			results, err := s.OnboardInitialUser(ctx, tt.args.request)
			if (err != nil) != (tt.wants.errCode != "") {
				t.Logf("Error: %v", err)
				t.Fatalf("expected error code '%s' got '%v'", tt.wants.errCode, err)
			}
			if err != nil && tt.wants.errCode != "" {
				if code := influxdb.ErrorCode(err); code != tt.wants.errCode {
					t.Logf("Error: %v", err)
					t.Fatalf("expected error code to match '%s' got '%v'", tt.wants.errCode, code)
				}
			}
			if diff := cmp.Diff(results, tt.wants.results, onboardCmpOptions); diff != "" {
				t.Errorf("onboarding results are different -got/+want\ndiff %s", diff)
			}
		})
	}

}

const (
	oneID    = "020f755c3c082000"
	twoID    = "020f755c3c082001"
	threeID  = "020f755c3c082002"
	fourID   = "020f755c3c082003"
	fiveID   = "020f755c3c082004"
	sixID    = "020f755c3c082005"
	oneToken = "020f755c3c082008"
)

type loopIDGenerator struct {
	s []string
	p int
}

func (g *loopIDGenerator) ID() influxdb.ID {
	if g.p == len(g.s) {
		g.p = 0
	}
	id := MustIDBase16(g.s[g.p])
	g.p++
	return id
}
