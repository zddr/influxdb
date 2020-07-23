package query

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"mime"
	"net/http"
	"strconv"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/influxdata/flux"
	"github.com/influxdata/influxdb/flux/client"
	"github.com/influxdata/influxdb/services/storage"
	kithttp "github.com/influxdata/influxdb/servicesv2/kit/http"
)

type QueryHandler struct {
	chi.Router
	api *kithttp.API

	queryService QueryService
}

// httpDialect is an encoding dialect that can write metadata to HTTP headers
type httpDialect interface {
	SetHeaders(w http.ResponseWriter)
}

const (
	v1Prefix = "/query"
	prefix   = "/api/v2/query"
)

func NewHTTPQueryHandler(s QueryService) *QueryHandler {
	svr := &QueryHandler{
		queryService: s,
	}

	r := chi.NewRouter()
	r.Use(
		middleware.Recoverer,
		middleware.RequestID,
		middleware.RealIP,
	)
	r.Post("/api/v2/query", svr.handleQuery)
	return svr
}

func (h *QueryHandler) HandleQuery(w http.ResponseWriter, r *http.Request) {
	req, err := decodeQueryRequest(r)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		h.api.Err(w, r, err)
		return
	}

	ctx := r.Context()
	if val := r.FormValue("node_id"); val != "" {
		if nodeID, err := strconv.ParseUint(val, 10, 64); err == nil {
			ctx = storage.NewContextWithReadOptions(ctx, &storage.ReadOptions{NodeID: nodeID})
		}
	}

	pr := req.ProxyRequest()

	// execute the query
	// wrap auth middleware here to check if user is allowed to query
	q, err := h.queryService.Query(ctx, pr.Compiler)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		h.api.Err(w, r, err)
		return
	}
	defer func() {
		q.Cancel()
		q.Done()
	}()

	// NOTE: We do not write out the headers here.
	// It is possible that if the encoding step fails
	// that we can write an error header so long as
	// the encoder did not write anything.
	// As such we rely on the http.ResponseWriter behavior
	// to write an StatusOK header with the first write.
	if hd, ok := pr.Dialect.(httpDialect); !ok {
		w.WriteHeader(http.StatusBadRequest)
		h.api.Err(w, r, fmt.Errorf("unsupported dialect over HTTP %T", req.Dialect))
		return
	} else {
		hd.SetHeaders(w)
	}
	encoder := pr.Dialect.Encoder()
	results := flux.NewResultIteratorFromQuery(q)
	defer results.Release()

	n, err := encoder.Encode(w, results)
	if err != nil {
		if n == 0 {
			// If the encoder did not write anything, we can write an error header.
			w.WriteHeader(http.StatusInternalServerError)
			h.api.Err(w, r, err)
		}
	}
}

func decodeQueryRequest(r *http.Request) (*client.QueryRequest, error) {
	ct := r.Header.Get("Content-Type")
	mt, _, err := mime.ParseMediaType(ct)
	if err != nil {
		return nil, err
	}

	var req client.QueryRequest
	switch mt {
	case "application/vnd.flux":
		if d, err := ioutil.ReadAll(r.Body); err != nil {
			return nil, err
		} else {
			req.Query = string(d)
		}
	default:
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			return nil, err
		}
	}

	req = req.WithDefaults()
	err = req.Validate()
	if err != nil {
		return nil, err
	}

	return &req, err
}
