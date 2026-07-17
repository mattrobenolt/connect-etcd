package etcd

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"connectrpc.com/connect"

	"go.withmatt.com/connect-etcd/internal/log"
	"go.withmatt.com/connect-etcd/types/etcdserverpb"
	"go.withmatt.com/connect-etcd/types/etcdserverpb/etcdserverpbconnect"
)

// fakeAuthClient stubs only Authenticate; the embedded nil interface panics if
// anything else is called.
type fakeAuthClient struct {
	etcdserverpbconnect.AuthClient
	authenticate func(context.Context, *connect.Request[etcdserverpb.AuthenticateRequest]) (*connect.Response[etcdserverpb.AuthenticateResponse], error)
}

func (f *fakeAuthClient) Authenticate(ctx context.Context, req *connect.Request[etcdserverpb.AuthenticateRequest]) (*connect.Response[etcdserverpb.AuthenticateResponse], error) {
	return f.authenticate(ctx, req)
}

func staticCredentials(username, password string) CredentialsProvider {
	return func() (string, string) { return username, password }
}

func tokenIssuer(t *testing.T, calls *int) *fakeAuthClient {
	t.Helper()
	return &fakeAuthClient{
		authenticate: func(_ context.Context, req *connect.Request[etcdserverpb.AuthenticateRequest]) (*connect.Response[etcdserverpb.AuthenticateResponse], error) {
			*calls++
			return connect.NewResponse(&etcdserverpb.AuthenticateResponse{
				Token: fmt.Sprintf("token-%s-%d", req.Msg.Name, *calls),
			}), nil
		},
	}
}

func TestTokenAuthInterceptorUnaryAuthenticatesOnceAndCachesToken(t *testing.T) {
	t.Parallel()

	var authCalls int
	interceptor := &tokenAuthInterceptor{
		credentials: staticCredentials("alice", "secret"),
		auth:        tokenIssuer(t, &authCalls),
		logger:      log.Default,
	}

	var got []string
	next := interceptor.WrapUnary(func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		got = append(got, req.Header().Get(tokenHeaderKey))
		return connect.NewResponse(&struct{}{}), nil
	})

	for range 3 {
		if _, err := next(context.Background(), connect.NewRequest(&struct{}{})); err != nil {
			t.Fatalf("next() error = %v", err)
		}
	}

	if authCalls != 1 {
		t.Fatalf("Authenticate calls = %d, want 1", authCalls)
	}
	for i, tok := range got {
		if tok != "token-alice-1" {
			t.Fatalf("request %d token = %q, want %q", i, tok, "token-alice-1")
		}
	}
}

func TestTokenAuthInterceptorUnaryReauthenticatesOnInvalidToken(t *testing.T) {
	t.Parallel()

	var authCalls int
	interceptor := &tokenAuthInterceptor{
		credentials: staticCredentials("alice", "secret"),
		auth:        tokenIssuer(t, &authCalls),
		logger:      log.Default,
	}

	var got []string
	next := interceptor.WrapUnary(func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		got = append(got, req.Header().Get(tokenHeaderKey))
		if req.Header().Get(tokenHeaderKey) == "token-alice-1" {
			return nil, connect.NewError(
				connect.CodeUnauthenticated,
				errors.New(errInvalidAuthToken),
			)
		}
		return connect.NewResponse(&struct{}{}), nil
	})

	if _, err := next(context.Background(), connect.NewRequest(&struct{}{})); err != nil {
		t.Fatalf("next() error = %v", err)
	}

	if authCalls != 2 {
		t.Fatalf("Authenticate calls = %d, want 2", authCalls)
	}
	want := []string{"token-alice-1", "token-alice-2"}
	if len(got) != len(want) {
		t.Fatalf("request attempts = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("attempt %d token = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestTokenAuthInterceptorUnaryReturnsErrorWhenReauthKeepsFailing(t *testing.T) {
	t.Parallel()

	interceptor := &tokenAuthInterceptor{
		credentials: staticCredentials("alice", "secret"),
		auth: &fakeAuthClient{
			authenticate: func(context.Context, *connect.Request[etcdserverpb.AuthenticateRequest]) (*connect.Response[etcdserverpb.AuthenticateResponse], error) {
				return connect.NewResponse(&etcdserverpb.AuthenticateResponse{Token: "tok"}), nil
			},
		},
		logger: log.Default,
	}

	invalid := connect.NewError(connect.CodeUnauthenticated, errors.New(errInvalidAuthToken))
	var attempts int
	next := interceptor.WrapUnary(func(context.Context, connect.AnyRequest) (connect.AnyResponse, error) {
		attempts++
		return nil, invalid
	})

	_, err := next(context.Background(), connect.NewRequest(&struct{}{}))
	if err == nil {
		t.Fatal("next() error = nil, want invalid auth token error")
	}
	// Exactly one re-auth retry: no infinite loop when the server keeps
	// rejecting fresh tokens.
	if attempts != 2 {
		t.Fatalf("request attempts = %d, want 2", attempts)
	}
}

func TestTokenAuthInterceptorRecoversWhenAuthGetsEnabled(t *testing.T) {
	t.Parallel()

	// Server starts with auth disabled, then enables it.
	authEnabled := false
	var authCalls int
	interceptor := &tokenAuthInterceptor{
		credentials: staticCredentials("alice", "secret"),
		auth: &fakeAuthClient{
			authenticate: func(context.Context, *connect.Request[etcdserverpb.AuthenticateRequest]) (*connect.Response[etcdserverpb.AuthenticateResponse], error) {
				authCalls++
				if !authEnabled {
					return nil, connect.NewError(
						connect.CodeFailedPrecondition,
						errors.New(errAuthNotEnabled),
					)
				}
				return connect.NewResponse(&etcdserverpb.AuthenticateResponse{Token: "tok-1"}), nil
			},
		},
		logger: log.Default,
	}

	var got []string
	next := interceptor.WrapUnary(func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		tok := req.Header().Get(tokenHeaderKey)
		got = append(got, tok)
		if authEnabled && tok == "" {
			return nil, connect.NewError(
				connect.CodeInvalidArgument,
				errors.New(errUserEmpty),
			)
		}
		return connect.NewResponse(&struct{}{}), nil
	})

	// Auth disabled: request goes through with no token, single Authenticate
	// attempt that learns auth is off.
	if _, err := next(context.Background(), connect.NewRequest(&struct{}{})); err != nil {
		t.Fatalf("next() error = %v", err)
	}
	if authCalls != 1 {
		t.Fatalf("Authenticate calls = %d, want 1", authCalls)
	}

	// Auth flips on server-side: the "user name is empty" rejection must
	// trigger a re-auth and a retried request with a real token.
	authEnabled = true
	if _, err := next(context.Background(), connect.NewRequest(&struct{}{})); err != nil {
		t.Fatalf("next() after auth enable error = %v", err)
	}

	want := []string{"", "", "tok-1"}
	if len(got) != len(want) {
		t.Fatalf("request attempts = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("attempt %d token = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestTokenAuthInterceptorReprobesAfterAuthDisabledTTL(t *testing.T) {
	t.Parallel()

	authEnabled := false
	var authCalls int
	interceptor := &tokenAuthInterceptor{
		credentials: staticCredentials("alice", "secret"),
		auth: &fakeAuthClient{
			authenticate: func(context.Context, *connect.Request[etcdserverpb.AuthenticateRequest]) (*connect.Response[etcdserverpb.AuthenticateResponse], error) {
				authCalls++
				if !authEnabled {
					return nil, connect.NewError(
						connect.CodeFailedPrecondition,
						errors.New(errAuthNotEnabled),
					)
				}
				return connect.NewResponse(&etcdserverpb.AuthenticateResponse{Token: "tok-1"}), nil
			},
		},
		logger: log.Default,
	}

	// First call learns auth is off.
	tok, err := interceptor.getToken(context.Background())
	if err != nil || tok != "" {
		t.Fatalf("getToken() = (%q, %v), want empty token, nil error", tok, err)
	}
	// Within the TTL the cached result is trusted: no re-probe.
	if tok, err := interceptor.getToken(context.Background()); err != nil || tok != "" {
		t.Fatalf("getToken() within TTL = (%q, %v), want empty token, nil error", tok, err)
	}
	if authCalls != 1 {
		t.Fatalf("Authenticate calls = %d, want 1 (no re-probe within TTL)", authCalls)
	}

	// Auth flips on server-side. Simulate TTL expiry: streams see watch-cancel
	// messages (not RPC errors), so this re-probe is their only recovery path.
	authEnabled = true
	interceptor.mu.Lock()
	interceptor.authDisabledAt = time.Now().Add(-authDisabledTTL)
	interceptor.mu.Unlock()

	tok, err = interceptor.getToken(context.Background())
	if err != nil || tok != "tok-1" {
		t.Fatalf("getToken() after TTL = (%q, %v), want (\"tok-1\", nil)", tok, err)
	}
	if authCalls != 2 {
		t.Fatalf("Authenticate calls = %d, want 2", authCalls)
	}
}

func TestTokenAuthInterceptorStreamingAttachesTokenAndResetsOnAuthError(t *testing.T) {
	t.Parallel()

	var authCalls int
	interceptor := &tokenAuthInterceptor{
		credentials: staticCredentials("watcher", "secret"),
		auth:        tokenIssuer(t, &authCalls),
		logger:      log.Default,
	}

	var conns []*stubStreamingClientConn
	wrapped := interceptor.WrapStreamingClient(func(ctx context.Context, spec connect.Spec) connect.StreamingClientConn {
		conn := &stubStreamingClientConn{
			requestHeader:   make(http.Header),
			responseHeader:  make(http.Header),
			responseTrailer: make(http.Header),
		}
		conns = append(conns, conn)
		return conn
	})

	stream1 := wrapped(context.Background(), connect.Spec{})
	if got := conns[0].requestHeader.Get(tokenHeaderKey); got != "token-watcher-1" {
		t.Fatalf("stream1 token = %q, want %q", got, "token-watcher-1")
	}

	// Server invalidates the token mid-stream (e.g. simple token TTL): the
	// Receive error must drop the cached token so the next stream re-auths.
	conns[0].receiveErr = connect.NewError(
		connect.CodeUnauthenticated,
		errors.New(errInvalidAuthToken),
	)
	if err := stream1.Receive(&struct{}{}); err == nil {
		t.Fatal("stream1.Receive() error = nil, want invalid auth token error")
	}

	wrapped(context.Background(), connect.Spec{})
	if got := conns[1].requestHeader.Get(tokenHeaderKey); got != "token-watcher-2" {
		t.Fatalf("stream2 token = %q, want %q", got, "token-watcher-2")
	}
	if authCalls != 2 {
		t.Fatalf("Authenticate calls = %d, want 2", authCalls)
	}
}

func TestClientInterceptorsIncludeCredentialsWithoutRetry(t *testing.T) {
	t.Parallel()

	interceptors := clientInterceptors(Config{
		RetryOptions: NoRetry,
		Credentials:  staticCredentials("user", "pass"),
	}, http.DefaultClient, "http://localhost:2379")

	if len(interceptors) != 1 {
		t.Fatalf("len(interceptors) = %d, want 1", len(interceptors))
	}
	if _, ok := interceptors[0].(*tokenAuthInterceptor); !ok {
		t.Fatalf("interceptors[0] = %T, want *tokenAuthInterceptor", interceptors[0])
	}
}

func TestClientInterceptorsOrderRetryBeforeCredentials(t *testing.T) {
	t.Parallel()

	interceptors := clientInterceptors(Config{
		RetryOptions: &RetryOptions{
			UnaryAttempts: 1,
		},
		Credentials: staticCredentials("user", "pass"),
	}, http.DefaultClient, "http://localhost:2379")

	if len(interceptors) != 2 {
		t.Fatalf("len(interceptors) = %d, want 2", len(interceptors))
	}
	if _, ok := interceptors[0].(connect.UnaryInterceptorFunc); !ok {
		t.Fatalf("interceptors[0] = %T, want connect.UnaryInterceptorFunc", interceptors[0])
	}
	if _, ok := interceptors[1].(*tokenAuthInterceptor); !ok {
		t.Fatalf("interceptors[1] = %T, want *tokenAuthInterceptor", interceptors[1])
	}
}

type stubStreamingClientConn struct {
	requestHeader   http.Header
	responseHeader  http.Header
	responseTrailer http.Header
	receiveErr      error
}

func (s *stubStreamingClientConn) Spec() connect.Spec {
	return connect.Spec{}
}

func (s *stubStreamingClientConn) Peer() connect.Peer {
	return connect.Peer{}
}

func (s *stubStreamingClientConn) Send(any) error {
	return nil
}

func (s *stubStreamingClientConn) RequestHeader() http.Header {
	return s.requestHeader
}

func (s *stubStreamingClientConn) CloseRequest() error {
	return nil
}

func (s *stubStreamingClientConn) Receive(any) error {
	return s.receiveErr
}

func (s *stubStreamingClientConn) ResponseHeader() http.Header {
	return s.responseHeader
}

func (s *stubStreamingClientConn) ResponseTrailer() http.Header {
	return s.responseTrailer
}

func (s *stubStreamingClientConn) CloseResponse() error {
	return nil
}
