package write

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/influxdata/influxdb/models"
	influxdb "github.com/influxdata/influxdb/servicesv2"
	kithttp "github.com/influxdata/influxdb/servicesv2/kit/http"
)

// WriteHandler represents an HTTP API handler for write requests.
type WriteHandler struct {
	chi.Router
	api      *kithttp.API
	writeSvc *Service

	bucketSvc influxdb.BucketService
	orgSvc    influxdb.OrganizationService
	// dbrpSvc   influxdb.DBRPMappingServiceV2
}

const (
	v1Prefix = "/write"
	prefix   = "/api/v2/write"
)

// NewHTTPWriteHandler constructs a new http server.
func NewHTTPWriteHandler(writeSvc *Service, orgSvc influxdb.OrganizationService, bucketSvc influxdb.BucketService) *WriteHandler {
	svr := &WriteHandler{
		api:       kithttp.NewAPI(),
		writeSvc:  writeSvc,
		bucketSvc: bucketSvc,
		orgSvc:    orgSvc,
	}

	r := chi.NewRouter()
	r.Use(
		middleware.Recoverer,
		middleware.RequestID,
		middleware.RealIP,
	)

	r.Post("/", svr.handleWrite)

	svr.Router = r
	return svr
}

type resourceHandler struct {
	prefix string
	*WriteHandler
}

func (h *resourceHandler) Prefix() string {
	return h.prefix
}
func (h *WriteHandler) v1ResourceHandler() *resourceHandler {
	return &resourceHandler{prefix: v1Prefix, WriteHandler: h}
}

func (h *WriteHandler) v2ResourceHandler() *resourceHandler {
	return &resourceHandler{prefix: prefix, WriteHandler: h}
}

func (h *WriteHandler) handleWrite(w http.ResponseWriter, r *http.Request) {
	// lookup bucket
	precision := chi.URLParam(r, "precision")
	switch precision {
	case "ns":
		precision = "n"
	case "us":
		precision = "u"
	case "ms", "s", "":
		// same as v1 so do nothing
	default:
		err := fmt.Errorf("invalid precision %q (use ns, us, ms or s)", precision)
		h.api.Err(w, r, err)
		return
	}
	org, err := h.findOrganization(r.Context(), r)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	bucket, err := h.findBucket(r.Context(), r, org.ID)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	// parse points
	points, err := h.parsePoints(r.Context(), r, precision)
	if err != nil {
		if err.Error() == "EOF" && len(points) == 0 {
			w.WriteHeader(http.StatusOK)
			return
		}
		h.api.Err(w, r, err)
		return
	}

	// write
	if err := h.writeSvc.WritePoints(r.Context(), bucket.ID, points); err != nil {
		h.api.Err(w, r, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *WriteHandler) parsePoints(ctx context.Context, r *http.Request, precision string) ([]models.Point, error) {
	body := r.Body
	// Handle gzip decoding of the body
	if r.Header.Get("Content-Encoding") == "gzip" {
		b, err := gzip.NewReader(r.Body)
		if err != nil {
			return nil, err
		}
		defer b.Close()
		body = b
	}

	var bs []byte
	buf := bytes.NewBuffer(bs)

	_, err := buf.ReadFrom(body)
	if err != nil {
		return nil, err
	}
	return models.ParsePointsWithPrecision(buf.Bytes(), time.Now().UTC(), precision)
}

func (h *WriteHandler) findOrganization(ctx context.Context, r *http.Request) (*influxdb.Organization, error) {
	filter := influxdb.OrganizationFilter{}
	if organization := r.URL.Query().Get("org"); organization != "" {
		if id, err := influxdb.IDFromString(organization); err == nil {
			filter.ID = id
		} else {
			filter.Name = &organization
		}
	}

	if reqID := r.URL.Query().Get("org_id"); reqID != "" {
		var err error
		filter.ID, err = influxdb.IDFromString(reqID)
		if err != nil {
			return nil, err
		}
	}
	return h.orgSvc.FindOrganization(ctx, filter)
}

func (h *WriteHandler) findBucket(ctx context.Context, r *http.Request, orgID influxdb.ID) (*influxdb.Bucket, error) {
	bucket := chi.URLParam(r, "bucket")
	if id, err := influxdb.IDFromString(bucket); err == nil {
		b, err := h.bucketSvc.FindBucket(ctx, influxdb.BucketFilter{
			OrganizationID: &orgID,
			ID:             id,
		})
		if err != nil && influxdb.ErrorCode(err) != influxdb.ENotFound {
			return nil, err
		} else if err == nil {
			return b, err
		}
	}

	return h.bucketSvc.FindBucket(ctx, influxdb.BucketFilter{
		OrganizationID: &orgID,
		Name:           &bucket,
	})
}
