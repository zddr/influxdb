package authorization

import (
	"context"

	influxdb "github.com/influxdata/influxdb/servicesv2"
)

// tenantService is a mock implementation of an authorization.tenantService
type tenantService struct {
	FindUserByIDFn        func(context.Context, influxdb.ID) (*influxdb.User, error)
	FindUserFn            func(context.Context, influxdb.UserFilter) (*influxdb.User, error)
	FindOrganizationByIDF func(ctx context.Context, id influxdb.ID) (*influxdb.Organization, error)
	FindOrganizationF     func(ctx context.Context, filter influxdb.OrganizationFilter) (*influxdb.Organization, error)
	FindBucketByIDFn      func(context.Context, influxdb.ID) (*influxdb.Bucket, error)
}

// FindUserByID returns a single User by ID.
func (s *tenantService) FindUserByID(ctx context.Context, id influxdb.ID) (*influxdb.User, error) {
	return s.FindUserByIDFn(ctx, id)
}

// FindUsers returns a list of Users that match filter and the total count of matching Users.
func (s *tenantService) FindUser(ctx context.Context, filter influxdb.UserFilter) (*influxdb.User, error) {
	return s.FindUserFn(ctx, filter)
}

//FindOrganizationByID calls FindOrganizationByIDF.
func (s *tenantService) FindOrganizationByID(ctx context.Context, id influxdb.ID) (*influxdb.Organization, error) {
	return s.FindOrganizationByIDF(ctx, id)
}

//FindOrganization calls FindOrganizationF.
func (s *tenantService) FindOrganization(ctx context.Context, filter influxdb.OrganizationFilter) (*influxdb.Organization, error) {
	return s.FindOrganizationF(ctx, filter)
}

func (s *tenantService) FindBucketByID(ctx context.Context, id influxdb.ID) (*influxdb.Bucket, error) {
	return s.FindBucketByIDFn(ctx, id)
}

// MockAuthorizationService is a mock implementation of a retention.AuthorizationService, which
// also makes it a suitable mock to use wherever an influxdb.AuthorizationService is required.
type MockAuthorizationService struct {
	// Methods for a retention.AuthorizationService
	OpenFn  func() error
	CloseFn func() error

	// Methods for an influxdb.AuthorizationService
	FindAuthorizationByIDFn    func(context.Context, influxdb.ID) (*influxdb.Authorization, error)
	FindAuthorizationByTokenFn func(context.Context, string) (*influxdb.Authorization, error)
	FindAuthorizationsFn       func(context.Context, influxdb.AuthorizationFilter, ...influxdb.FindOptions) ([]*influxdb.Authorization, int, error)
	CreateAuthorizationFn      func(context.Context, *influxdb.Authorization) error
	DeleteAuthorizationFn      func(context.Context, influxdb.ID) error
	UpdateAuthorizationFn      func(context.Context, influxdb.ID, *influxdb.AuthorizationUpdate) (*influxdb.Authorization, error)
}

// NewMockAuthorizationService returns a MockAuthorizationService where its methods will return
// zero values.
func NewMockAuthorizationService() *MockAuthorizationService {
	return &MockAuthorizationService{
		FindAuthorizationByIDFn:    func(context.Context, influxdb.ID) (*influxdb.Authorization, error) { return nil, nil },
		FindAuthorizationByTokenFn: func(context.Context, string) (*influxdb.Authorization, error) { return nil, nil },
		FindAuthorizationsFn: func(context.Context, influxdb.AuthorizationFilter, ...influxdb.FindOptions) ([]*influxdb.Authorization, int, error) {
			return nil, 0, nil
		},
		CreateAuthorizationFn: func(context.Context, *influxdb.Authorization) error { return nil },
		DeleteAuthorizationFn: func(context.Context, influxdb.ID) error { return nil },
		UpdateAuthorizationFn: func(context.Context, influxdb.ID, *influxdb.AuthorizationUpdate) (*influxdb.Authorization, error) {
			return nil, nil
		},
	}
}

// FindAuthorizationByID returns a single authorization by ID.
func (s *MockAuthorizationService) FindAuthorizationByID(ctx context.Context, id influxdb.ID) (*influxdb.Authorization, error) {
	return s.FindAuthorizationByIDFn(ctx, id)
}

func (s *MockAuthorizationService) FindAuthorizationByToken(ctx context.Context, t string) (*influxdb.Authorization, error) {
	return s.FindAuthorizationByTokenFn(ctx, t)
}

// FindAuthorizations returns a list of authorizations that match filter and the total count of matching authorizations.
func (s *MockAuthorizationService) FindAuthorizations(ctx context.Context, filter influxdb.AuthorizationFilter, opts ...influxdb.FindOptions) ([]*influxdb.Authorization, int, error) {
	return s.FindAuthorizationsFn(ctx, filter, opts...)
}

// CreateAuthorization creates a new authorization and sets b.ID with the new identifier.
func (s *MockAuthorizationService) CreateAuthorization(ctx context.Context, authorization *influxdb.Authorization) error {
	return s.CreateAuthorizationFn(ctx, authorization)
}

// DeleteAuthorization removes a authorization by ID.
func (s *MockAuthorizationService) DeleteAuthorization(ctx context.Context, id influxdb.ID) error {
	return s.DeleteAuthorizationFn(ctx, id)
}

// UpdateAuthorization updates the status and description if available.
func (s *MockAuthorizationService) UpdateAuthorization(ctx context.Context, id influxdb.ID, upd *influxdb.AuthorizationUpdate) (*influxdb.Authorization, error) {
	return s.UpdateAuthorizationFn(ctx, id, upd)
}
