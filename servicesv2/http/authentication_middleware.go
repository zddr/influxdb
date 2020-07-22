package http

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/influxdata/httprouter"
	influxdb "github.com/influxdata/influxdb/servicesv2"
	icontext "github.com/influxdata/influxdb/servicesv2/context"
	"github.com/influxdata/influxdb/servicesv2/jsonweb"
	"github.com/opentracing/opentracing-go"
	"go.uber.org/zap"
)

// AuthenticationHandler is a middleware for authenticating incoming requests.
type AuthenticationHandler struct {
	influxdb.HTTPErrorHandler
	log *zap.Logger

	authorizationService influxdb.AuthorizationService
	// sessionService       influxdb.SessionService
	userService          influxdb.UserService
	tokenParser          *jsonweb.TokenParser
	sessionRenewDisabled bool

	// This is only really used for it's lookup method the specific http
	// handler used to register routes does not matter.
	noAuthRouter *httprouter.Router

	Handler http.Handler
}

// todo (al) add session svc

// NewAuthenticationHandler creates an authentication handler.
func NewAuthenticationHandler(log *zap.Logger, h influxdb.HTTPErrorHandler, authSvc *influxdb.AuthorizationService, userSvc *influxdb.UserService) *AuthenticationHandler {
	return &AuthenticationHandler{
		log:                  log,
		HTTPErrorHandler:     h,
		Handler:              http.DefaultServeMux,
		authorizationService: *authSvc,
		userService:          *userSvc,
		tokenParser:          jsonweb.NewTokenParser(jsonweb.EmptyKeyStore),
		noAuthRouter:         httprouter.New(), // todo (al) use chi router
	}
}

// func AuthMiddleware() kithttp.Middleware {
// 	return func(next http.Handler) http.Handler {
// 		fn := func(w http.ResponseWriter, r *http.Request) {
// 			next.ServeHTTP(w, r)
// 		}
// 		return http.HandlerFunc(fn)
// 	}
// }

// // func Wrap(next http.Handler) kithttp.Middleware {
// // 	return wrap(next)
// // }

// // func wrap(next http.Handler) http.Handler {
// // 	fn := func(w http.ResponseWriter, r *http.Request) {
// // 		// statusW := NewStatusResponseWriter(w)

// // 		next.ServeHTTP(w, r)
// // 	}
// // 	return http.HandlerFunc(fn)
// // }

// RegisterNoAuthRoute excludes routes from needing authentication.
func (h *AuthenticationHandler) RegisterNoAuthRoute(method, path string) {
	// the handler specified here does not matter.
	fmt.Println("registering no auth: ", path)
	h.noAuthRouter.HandlerFunc(method, path, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
}

const (
	tokenAuthScheme   = "token"
	sessionAuthScheme = "session"
)

// ProbeAuthScheme probes the http request for the requests for token or cookie session.
func ProbeAuthScheme(r *http.Request) (string, error) {
	_, tokenErr := GetToken(r)
	// _, sessErr := decodeCookieSession(r.Context(), r)

	if tokenErr != nil { //&& sessErr != nil todo (al)
		return "", fmt.Errorf("token required")
	}

	if tokenErr == nil {
		return tokenAuthScheme, nil
	}

	return tokenAuthScheme, nil
}

func (h *AuthenticationHandler) unauthorized(ctx context.Context, w http.ResponseWriter, err error) {
	h.log.Info("Unauthorized", zap.Error(err))
	UnauthorizedError(ctx, h, w)
}

// MiddlewareHandler extracts the session or token from the http request and places the resulting authorizer on the request context.
func (h *AuthenticationHandler) MiddlewareHandler(next http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		if noAuth, _, _ := h.noAuthRouter.Lookup(r.Method, r.URL.Path); noAuth != nil {
			next.ServeHTTP(w, r)
			return
		}

		ctx := r.Context()
		scheme, err := ProbeAuthScheme(r)
		if err != nil {
			h.unauthorized(ctx, w, err)
			return
		}

		var auth influxdb.Authorizer
		switch scheme {
		case tokenAuthScheme:
			auth, err = h.extractAuthorization(ctx, r)
		// case sessionAuthScheme: // todo (al) use session
		// 	auth, err = h.extractSession(ctx, r)
		default:
			// TODO: this error will be nil if it gets here, this should be remedied with some
			//  sentinel error I'm thinking
			err = errors.New("invalid auth scheme")
		}
		if err != nil {
			h.unauthorized(ctx, w, err)
			return
		}

		// jwt based auth is permission based rather than identity based
		// and therefor has no associated user. if the user ID is invalid
		// disregard the user active check
		if auth.GetUserID().Valid() {
			if err = h.isUserActive(ctx, auth); err != nil {
				InactiveUserError(ctx, h, w)
				return
			}
		}

		ctx = icontext.SetAuthorizer(ctx, auth)

		if span := opentracing.SpanFromContext(ctx); span != nil {
			span.SetTag("user_id", auth.GetUserID().String())
		}
		next.ServeHTTP(w, r.WithContext(ctx))
	}

	return http.HandlerFunc(fn)
}

func (h *AuthenticationHandler) isUserActive(ctx context.Context, auth influxdb.Authorizer) error {
	u, err := h.userService.FindUserByID(ctx, auth.GetUserID())
	if err != nil {
		return err
	}

	if u.Status != "inactive" {
		return nil
	}

	return &influxdb.Error{Code: influxdb.EForbidden, Msg: "User is inactive"}
}

func (h *AuthenticationHandler) extractAuthorization(ctx context.Context, r *http.Request) (influxdb.Authorizer, error) {
	t, err := GetToken(r)
	if err != nil {
		return nil, err
	}

	token, err := h.tokenParser.Parse(t)
	if err == nil {
		return token, nil
	}

	// if the error returned signifies ths token is
	// not a well formed JWT then use it as a lookup
	// key for its associated authorization
	// otherwise return the error
	if !jsonweb.IsMalformedError(err) {
		return nil, err
	}

	return h.authorizationService.FindAuthorizationByToken(ctx, t)
}

// func (h *AuthenticationHandler) extractSession(ctx context.Context, r *http.Request) (*influxdb.Session, error) {
// 	k, err := decodeCookieSession(ctx, r)
// 	if err != nil {
// 		return nil, err
// 	}

// 	s, err := h.SessionService.FindSession(ctx, k)
// 	if err != nil {
// 		return nil, err
// 	}

// 	if !h.SessionRenewDisabled {
// 		// if the session is not expired, renew the session
// 		err = h.SessionService.RenewSession(ctx, s, time.Now().Add(influxdb.RenewSessionTime))
// 		if err != nil {
// 			return nil, err
// 		}
// 	}

// 	return s, err
// }
