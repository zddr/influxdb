package testing

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	influxdb "github.com/influxdata/influxdb/servicesv2"
)

var userResourceMappingCmpOptions = cmp.Options{
	cmp.Comparer(func(x, y []byte) bool {
		return bytes.Equal(x, y)
	}),
	cmp.Transformer("Sort", func(in []*influxdb.UserResourceMapping) []*influxdb.UserResourceMapping {
		out := append([]*influxdb.UserResourceMapping(nil), in...)
		sort.Slice(out, func(i, j int) bool {
			return out[i].ResourceID.String() > out[j].ResourceID.String()
		})
		return out
	}),
}

var mappingCmpOptions = cmp.Options{
	cmp.Comparer(func(x, y []byte) bool {
		return bytes.Equal(x, y)
	}),
	cmp.Transformer("Sort", func(in []*influxdb.UserResourceMapping) []*influxdb.UserResourceMapping {
		out := append([]*influxdb.UserResourceMapping(nil), in...) // Copy input to avoid mutating it
		sort.Slice(out, func(i, j int) bool {
			return out[i].ResourceID.String() > out[j].ResourceID.String()
		})
		return out
	}),
}

// UserResourceFields includes prepopulated data for mapping tests
type UserResourceFields struct {
	Organizations        []*influxdb.Organization
	Users                []*influxdb.User
	Buckets              []*influxdb.Bucket
	UserResourceMappings []*influxdb.UserResourceMapping
}

type userResourceMappingServiceF func(
	init func(UserResourceFields, *testing.T) (influxdb.UserResourceMappingService, func()),
	t *testing.T,
)

// UserResourceMappingService tests all the service functions.
func UserResourceMappingService(
	init func(UserResourceFields, *testing.T) (influxdb.UserResourceMappingService, func()),
	t *testing.T,
) {
	tests := []struct {
		name string
		fn   userResourceMappingServiceF
	}{
		{
			name: "CreateUserResourceMapping",
			fn:   CreateUserResourceMapping,
		},
		{
			name: "FindUserResourceMappings",
			fn:   FindUserResourceMappings,
		},
		{
			name: "DeleteUserResourceMapping",
			fn:   DeleteUserResourceMapping,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt := tt
			t.Parallel()
			tt.fn(init, t)
		})
	}
}

// baseUserResourceFields creates base fields to create URMs.
// Users for URMs must exist in order not to fail on creation.
func baseUserResourceFields() UserResourceFields {
	return UserResourceFields{
		Users: []*influxdb.User{
			{
				Name: "user1",
				ID:   MustIDBase16(userOneID),
			},
			{
				Name: "user2",
				ID:   MustIDBase16(userTwoID),
			},
		},
	}
}

func CreateUserResourceMapping(
	init func(UserResourceFields, *testing.T) (influxdb.UserResourceMappingService, func()),
	t *testing.T,
) {
	type args struct {
		mapping *influxdb.UserResourceMapping
	}
	type wants struct {
		err      error
		mappings []*influxdb.UserResourceMapping
	}

	tests := []struct {
		name   string
		fields UserResourceFields
		args   args
		wants  wants
	}{
		{
			name: "basic create user resource mapping",
			fields: func() UserResourceFields {
				f := baseUserResourceFields()
				f.UserResourceMappings = []*influxdb.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(bucketOneID),
						UserID:       MustIDBase16(userOneID),
						UserType:     influxdb.Member,
						ResourceType: influxdb.BucketsResourceType,
					},
				}
				return f
			}(),
			args: args{
				mapping: &influxdb.UserResourceMapping{
					ResourceID:   MustIDBase16(bucketTwoID),
					UserID:       MustIDBase16(userTwoID),
					UserType:     influxdb.Member,
					ResourceType: influxdb.BucketsResourceType,
				},
			},
			wants: wants{
				mappings: []*influxdb.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(bucketOneID),
						UserID:       MustIDBase16(userOneID),
						UserType:     influxdb.Member,
						ResourceType: influxdb.BucketsResourceType,
					},
					{
						ResourceID:   MustIDBase16(bucketTwoID),
						UserID:       MustIDBase16(userTwoID),
						UserType:     influxdb.Member,
						ResourceType: influxdb.BucketsResourceType,
					},
				},
			},
		},
		{
			name: "duplicate mappings are not allowed",
			fields: func() UserResourceFields {
				f := baseUserResourceFields()
				f.UserResourceMappings = []*influxdb.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(bucketOneID),
						UserID:       MustIDBase16(userOneID),
						UserType:     influxdb.Member,
						ResourceType: influxdb.BucketsResourceType,
					},
				}
				return f
			}(),
			args: args{
				mapping: &influxdb.UserResourceMapping{
					ResourceID:   MustIDBase16(bucketOneID),
					UserID:       MustIDBase16(userOneID),
					UserType:     influxdb.Member,
					ResourceType: influxdb.BucketsResourceType,
				},
			},
			wants: wants{
				mappings: []*influxdb.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(bucketOneID),
						UserID:       MustIDBase16(userOneID),
						UserType:     influxdb.Member,
						ResourceType: influxdb.BucketsResourceType,
					},
				},
				//lint:ignore ST1005 Error is capitalized in the tested code.
				err: fmt.Errorf("Unexpected error when assigning user to a resource: mapping for user %s already exists", userOneID),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()
			err := s.CreateUserResourceMapping(ctx, tt.args.mapping)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error messages to match '%v' got '%v'", tt.wants.err, err.Error())
				}
			}
			defer s.DeleteUserResourceMapping(ctx, tt.args.mapping.ResourceID, tt.args.mapping.UserID)

			mappings, _, err := s.FindUserResourceMappings(ctx, influxdb.UserResourceMappingFilter{})
			if err != nil {
				t.Fatalf("failed to retrieve mappings: %v", err)
			}
			if diff := cmp.Diff(mappings, tt.wants.mappings, mappingCmpOptions...); diff != "" {
				t.Errorf("mappings are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

func DeleteUserResourceMapping(
	init func(UserResourceFields, *testing.T) (influxdb.UserResourceMappingService, func()),
	t *testing.T,
) {
	type args struct {
		resourceID influxdb.ID
		userID     influxdb.ID
	}
	type wants struct {
		err      error
		mappings []*influxdb.UserResourceMapping
	}

	tests := []struct {
		name   string
		fields UserResourceFields
		args   args
		wants  wants
	}{
		{
			name: "basic delete user resource mapping",
			fields: func() UserResourceFields {
				f := baseUserResourceFields()
				f.UserResourceMappings = []*influxdb.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(bucketOneID),
						UserID:       MustIDBase16(userOneID),
						UserType:     influxdb.Member,
						ResourceType: influxdb.BucketsResourceType,
					},
				}
				return f
			}(),
			args: args{
				resourceID: MustIDBase16(bucketOneID),
				userID:     MustIDBase16(userOneID),
			},
			wants: wants{
				mappings: []*influxdb.UserResourceMapping{},
			},
		},
		{
			name: "deleting a non-existent user",
			fields: UserResourceFields{
				UserResourceMappings: []*influxdb.UserResourceMapping{},
			},
			args: args{
				resourceID: MustIDBase16(bucketOneID),
				userID:     MustIDBase16(userOneID),
			},
			wants: wants{
				mappings: []*influxdb.UserResourceMapping{},
				err:      fmt.Errorf("user to resource mapping not found"),
			},
		},
		{
			name: "delete user resource mapping for org",
			fields: UserResourceFields{
				Organizations: []*influxdb.Organization{
					{
						ID:   MustIDBase16(orgOneID),
						Name: "organization1",
					},
				},
				Users: []*influxdb.User{
					{
						ID:   MustIDBase16(userOneID),
						Name: "user1",
					},
				},
				Buckets: []*influxdb.Bucket{
					{
						ID:    MustIDBase16(bucketOneID),
						Name:  "bucket1",
						OrgID: MustIDBase16(orgOneID),
					},
				},
				UserResourceMappings: []*influxdb.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(orgOneID),
						ResourceType: influxdb.OrgsResourceType,
						MappingType:  influxdb.UserMappingType,
						UserID:       MustIDBase16(userOneID),
						UserType:     influxdb.Member,
					},
				},
			},
			args: args{
				resourceID: MustIDBase16(orgOneID),
				userID:     MustIDBase16(userOneID),
			},
			wants: wants{
				mappings: []*influxdb.UserResourceMapping{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()
			err := s.DeleteUserResourceMapping(ctx, tt.args.resourceID, tt.args.userID)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error messages to match '%v' got '%v'", tt.wants.err, err.Error())
				}
			}

			mappings, _, err := s.FindUserResourceMappings(ctx, influxdb.UserResourceMappingFilter{})
			if err != nil {
				t.Fatalf("failed to retrieve mappings: %v", err)
			}
			if diff := cmp.Diff(mappings, tt.wants.mappings, mappingCmpOptions...); diff != "" {
				t.Errorf("mappings are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

func FindUserResourceMappings(
	init func(UserResourceFields, *testing.T) (influxdb.UserResourceMappingService, func()),
	t *testing.T,
) {
	type args struct {
		filter influxdb.UserResourceMappingFilter
	}
	type wants struct {
		err      error
		mappings []*influxdb.UserResourceMapping
	}

	tests := []struct {
		name   string
		fields UserResourceFields
		args   args
		wants  wants
	}{
		{
			name: "basic find mappings",
			fields: func() UserResourceFields {
				f := baseUserResourceFields()
				f.UserResourceMappings = []*influxdb.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(bucketOneID),
						UserID:       MustIDBase16(userOneID),
						UserType:     influxdb.Member,
						ResourceType: influxdb.BucketsResourceType,
					},
					{
						ResourceID:   MustIDBase16(bucketTwoID),
						UserID:       MustIDBase16(userTwoID),
						UserType:     influxdb.Member,
						ResourceType: influxdb.BucketsResourceType,
					},
				}
				return f
			}(),
			args: args{
				filter: influxdb.UserResourceMappingFilter{},
			},
			wants: wants{
				mappings: []*influxdb.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(bucketOneID),
						UserID:       MustIDBase16(userOneID),
						UserType:     influxdb.Member,
						ResourceType: influxdb.BucketsResourceType,
					},
					{
						ResourceID:   MustIDBase16(bucketTwoID),
						UserID:       MustIDBase16(userTwoID),
						UserType:     influxdb.Member,
						ResourceType: influxdb.BucketsResourceType,
					},
				},
			},
		},
		{
			name: "find mappings filtered by user",
			fields: func() UserResourceFields {
				f := baseUserResourceFields()
				f.UserResourceMappings = []*influxdb.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(bucketOneID),
						UserID:       MustIDBase16(userOneID),
						UserType:     influxdb.Member,
						ResourceType: influxdb.BucketsResourceType,
					},
					{
						ResourceID:   MustIDBase16(bucketTwoID),
						UserID:       MustIDBase16(userTwoID),
						UserType:     influxdb.Member,
						ResourceType: influxdb.BucketsResourceType,
					},
				}
				return f
			}(),
			args: args{
				filter: influxdb.UserResourceMappingFilter{
					UserID: MustIDBase16(userOneID),
				},
			},
			wants: wants{
				mappings: []*influxdb.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(bucketOneID),
						UserID:       MustIDBase16(userOneID),
						UserType:     influxdb.Member,
						ResourceType: influxdb.BucketsResourceType,
					},
				},
			},
		},
		{
			name: "find mappings filtered by resource",
			fields: func() UserResourceFields {
				f := baseUserResourceFields()
				f.UserResourceMappings = []*influxdb.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(bucketOneID),
						UserID:       MustIDBase16(userOneID),
						UserType:     influxdb.Member,
						ResourceType: influxdb.BucketsResourceType,
					},
					{
						ResourceID:   MustIDBase16(bucketTwoID),
						UserID:       MustIDBase16(userTwoID),
						UserType:     influxdb.Member,
						ResourceType: influxdb.BucketsResourceType,
					},
				}
				return f
			}(),
			args: args{
				filter: influxdb.UserResourceMappingFilter{
					ResourceID: MustIDBase16(bucketOneID),
				},
			},
			wants: wants{
				mappings: []*influxdb.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(bucketOneID),
						UserID:       MustIDBase16(userOneID),
						UserType:     influxdb.Member,
						ResourceType: influxdb.BucketsResourceType,
					},
				},
			},
		},
		{
			name: "find mappings filtered by user type",
			fields: func() UserResourceFields {
				f := baseUserResourceFields()
				f.UserResourceMappings = []*influxdb.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(bucketOneID),
						UserID:       MustIDBase16(userOneID),
						UserType:     influxdb.Member,
						ResourceType: influxdb.BucketsResourceType,
					},
					{
						ResourceID:   MustIDBase16(bucketTwoID),
						UserID:       MustIDBase16(userTwoID),
						UserType:     influxdb.Owner,
						ResourceType: influxdb.BucketsResourceType,
					},
				}
				return f
			}(),
			args: args{
				filter: influxdb.UserResourceMappingFilter{
					UserType: influxdb.Owner,
				},
			},
			wants: wants{
				mappings: []*influxdb.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(bucketTwoID),
						UserID:       MustIDBase16(userTwoID),
						UserType:     influxdb.Owner,
						ResourceType: influxdb.BucketsResourceType,
					},
				},
			},
		},
		{
			name: "find mappings filtered by resource type",
			fields: func() UserResourceFields {
				f := baseUserResourceFields()
				f.UserResourceMappings = []*influxdb.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(bucketTwoID),
						UserID:       MustIDBase16(userTwoID),
						UserType:     influxdb.Member,
						ResourceType: influxdb.BucketsResourceType,
					},
				}
				return f
			}(),
			args: args{
				filter: influxdb.UserResourceMappingFilter{
					ResourceType: influxdb.BucketsResourceType,
				},
			},
			wants: wants{
				mappings: []*influxdb.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(bucketTwoID),
						UserID:       MustIDBase16(userTwoID),
						UserType:     influxdb.Member,
						ResourceType: influxdb.BucketsResourceType,
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
			mappings, _, err := s.FindUserResourceMappings(ctx, tt.args.filter)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error messages to match '%v' got '%v'", tt.wants.err, err.Error())
				}
			}

			if diff := cmp.Diff(mappings, tt.wants.mappings, mappingCmpOptions...); diff != "" {
				t.Errorf("mappings are different -got/+want\ndiff %s", diff)
			}
		})
	}
}
