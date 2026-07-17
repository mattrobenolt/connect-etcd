package etcd

import (
	"context"
	"net/http"
	"strings"
	"sync"

	"connectrpc.com/connect"

	"go.withmatt.com/connect-etcd/internal/log"
	"go.withmatt.com/connect-etcd/types/etcdserverpb"
	"go.withmatt.com/connect-etcd/types/etcdserverpb/etcdserverpbconnect"
)

// tokenHeaderKey is the gRPC metadata key etcd inspects for an auth token.
// See rpctypes.TokenFieldNameGRPC in etcd.
const tokenHeaderKey = "token"

// etcd error strings, stable across versions
// (see go.etcd.io/etcd/api/v3rpc/rpctypes).
const (
	errInvalidAuthToken = "etcdserver: invalid auth token"
	errAuthOldRevision  = "etcdserver: revision of auth store is old"
	errAuthNotEnabled   = "etcdserver: authentication is not enabled"
	errUserEmpty        = "etcdserver: user name is empty"
)

// needsReauth reports whether err indicates our token (or lack of one) was
// rejected and a fresh Authenticate call may fix it:
//
//   - "invalid auth token": simple tokens expire (5m default), are invalidated
//     by any auth store change, and are member-local so a reconnect can land on
//     a member that never issued ours.
//   - "revision of auth store is old": the token embeds an auth store revision
//     that has since changed.
//   - "user name is empty": we sent no token because we believed auth was
//     disabled, but it has been enabled since.
func needsReauth(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, errInvalidAuthToken) ||
		strings.Contains(msg, errAuthOldRevision) ||
		strings.Contains(msg, errUserEmpty)
}

func isAuthNotEnabled(err error) bool {
	return err != nil && strings.Contains(err.Error(), errAuthNotEnabled)
}

// tokenAuthInterceptor implements etcd's token-based authentication: it
// exchanges the credentials for a token via the Auth.Authenticate RPC, sends
// that token in the request metadata, and transparently re-authenticates when
// the server rejects the token.
//
// If the server reports authentication is not enabled, requests proceed
// without a token; the interceptor recovers automatically (via needsReauth)
// once auth is enabled server-side.
type tokenAuthInterceptor struct {
	credentials CredentialsProvider
	auth        etcdserverpbconnect.AuthClient
	logger      log.LeveledLogger

	// mu also serializes Authenticate calls so concurrent requests that race
	// on an expired token trigger a single re-authentication.
	mu           sync.Mutex
	token        string
	authDisabled bool
}

// getToken returns the cached token, authenticating first if needed. An empty
// token with nil error means auth is disabled server-side and the request
// should proceed without a token header.
func (i *tokenAuthInterceptor) getToken(ctx context.Context) (string, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.token != "" || i.authDisabled {
		return i.token, nil
	}
	username, password := i.credentials()
	res, err := i.auth.Authenticate(ctx, connect.NewRequest(&etcdserverpb.AuthenticateRequest{
		Name:     username,
		Password: password,
	}))
	if err != nil {
		if isAuthNotEnabled(err) {
			i.logger.Info("etcd auth: authentication not enabled on server, proceeding without token")
			i.authDisabled = true
			return "", nil
		}
		return "", err
	}
	i.token = res.Msg.Token
	return i.token, nil
}

// reset drops the cached token so the next request re-authenticates. It only
// clears the token that failed, so a token already refreshed by a concurrent
// request is kept.
func (i *tokenAuthInterceptor) reset(failed string) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.token == failed {
		i.token = ""
	}
	i.authDisabled = false
}

func setTokenHeader(header http.Header, token string) {
	if token == "" {
		header.Del(tokenHeaderKey)
		return
	}
	header.Set(tokenHeaderKey, token)
}

func (i *tokenAuthInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		token, err := i.getToken(ctx)
		if err != nil {
			return nil, err
		}
		setTokenHeader(req.Header(), token)
		res, err := next(ctx, req)
		if !needsReauth(err) {
			return res, err
		}
		i.reset(token)
		token, terr := i.getToken(ctx)
		if terr != nil {
			return nil, terr
		}
		setTokenHeader(req.Header(), token)
		return next(ctx, req)
	}
}

func (i *tokenAuthInterceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return func(ctx context.Context, spec connect.Spec) connect.StreamingClientConn {
		conn := next(ctx, spec)
		token, err := i.getToken(ctx)
		if err != nil {
			// We cannot return an error here; leave the stream without a token
			// so the server rejects it and the caller's retry loop comes back
			// through this path with a fresh Authenticate attempt.
			i.logger.Warn("etcd auth: failed to authenticate for stream",
				"error", err.Error(),
			)
			return conn
		}
		setTokenHeader(conn.RequestHeader(), token)
		return &tokenStreamConn{StreamingClientConn: conn, auth: i, token: token}
	}
}

func (i *tokenAuthInterceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return next
}

// tokenStreamConn watches for auth rejections on an established stream and
// drops the cached token so the caller's stream retry re-authenticates.
type tokenStreamConn struct {
	connect.StreamingClientConn
	auth  *tokenAuthInterceptor
	token string
}

func (c *tokenStreamConn) Send(msg any) error {
	err := c.StreamingClientConn.Send(msg)
	if needsReauth(err) {
		c.auth.reset(c.token)
	}
	return err
}

func (c *tokenStreamConn) Receive(msg any) error {
	err := c.StreamingClientConn.Receive(msg)
	if needsReauth(err) {
		c.auth.reset(c.token)
	}
	return err
}
