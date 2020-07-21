package tenant

import (
	influxdb "github.com/influxdata/influxdb/servicesv2"
	"go.uber.org/zap"
)

type Service struct {
	store *Store
}

func NewService(st *Store) influxdb.TenantService {
	return &Service{
		store: st,
	}
}

// TODO (al): add back in when we have HTTP code
type TenantSystem struct {
	UserSvc     influxdb.UserService
	PasswordSvc influxdb.PasswordsService
	UrmSvc      influxdb.UserResourceMappingService
	OrgSvc      influxdb.OrganizationService
	BucketSvc   influxdb.BucketService
	TenantSvc   influxdb.TenantService
}

func NewSystem(store *Store) *TenantSystem {
	ts := NewService(store)
	return &TenantSystem{
		UserSvc:     ts,
		PasswordSvc: ts,
		UrmSvc:      ts,
		OrgSvc:      ts,
		BucketSvc:   ts,
		TenantSvc:   ts,
	}
}

func (ts *TenantSystem) NewOrgHTTPHandler(log *zap.Logger) *OrgHandler {
	// secretHandler := secret.NewHandler(log, "id", secret.NewAuthedService(secretSvc))
	// urmHandler := NewURMHandler(log.With(zap.String("handler", "urm")), influxdb.OrgsResourceType, "id", ts.UserSvc, NewAuthedURMService(ts.OrgSvc, ts.UrmSvc))
	// labelHandler := label.NewHTTPEmbeddedHandler(log.With(zap.String("handler", "label")), influxdb.OrgsResourceType, labelSvc)
	return NewHTTPOrgHandler(log, NewAuthedOrgService(ts.OrgSvc))
}

func (ts *TenantSystem) NewBucketHTTPHandler(log *zap.Logger) *BucketHandler {
	// urmHandler := NewURMHandler(log.With(zap.String("handler", "urm")), influxdb.OrgsResourceType, "id", ts.UserSvc, NewAuthedURMService(ts.OrgSvc, ts.UrmSvc))
	// labelHandler := label.NewHTTPEmbeddedHandler(log.With(zap.String("handler", "label")), influxdb.BucketsResourceType, labelSvc)
	return NewHTTPBucketHandler(log, NewAuthedBucketService(ts.BucketSvc))
}

func (ts *TenantSystem) NewUserHTTPHandler(log *zap.Logger) *UserHandler {
	return NewHTTPUserHandler(log.With(zap.String("handler", "user")), NewAuthedUserService(ts.UserSvc), NewAuthedPasswordService(ts.PasswordSvc))
}
