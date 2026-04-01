package etcd

import (
	"context"
	"encoding/base64"
	"net/http"
	"testing"

	"connectrpc.com/connect"
)

func TestBasicAuthInterceptorWrapUnarySetsAuthorizationHeaderPerRequest(t *testing.T) {
	t.Parallel()

	credentials := []struct {
		username string
		password string
	}{
		{username: "alice", password: "first"},
		{username: "bob", password: "second"},
	}
	var calls int

	interceptor := &basicAuthInterceptor{
		credentials: func() (string, string) {
			creds := credentials[calls]
			calls++
			return creds.username, creds.password
		},
	}

	var got []string
	next := interceptor.WrapUnary(func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		got = append(got, req.Header().Get("Authorization"))
		return connect.NewResponse(&struct{}{}), nil
	})

	for range credentials {
		if _, err := next(context.Background(), connect.NewRequest(&struct{}{})); err != nil {
			t.Fatalf("next() error = %v", err)
		}
	}

	want := []string{
		basicAuthHeader("alice", "first"),
		basicAuthHeader("bob", "second"),
	}
	if calls != len(credentials) {
		t.Fatalf("credentials provider calls = %d, want %d", calls, len(credentials))
	}
	if len(got) != len(want) {
		t.Fatalf("captured headers = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("header %d = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestBasicAuthInterceptorWrapStreamingClientSetsAuthorizationHeaderPerStream(t *testing.T) {
	t.Parallel()

	credentials := []struct {
		username string
		password string
	}{
		{username: "watcher", password: "first"},
		{username: "watcher", password: "rotated"},
	}
	var calls int

	interceptor := &basicAuthInterceptor{
		credentials: func() (string, string) {
			creds := credentials[calls]
			calls++
			return creds.username, creds.password
		},
	}

	wrapped := interceptor.WrapStreamingClient(func(ctx context.Context, spec connect.Spec) connect.StreamingClientConn {
		return &stubStreamingClientConn{
			requestHeader:   make(http.Header),
			responseHeader:  make(http.Header),
			responseTrailer: make(http.Header),
		}
	})

	stream1 := wrapped(context.Background(), connect.Spec{})
	stream2 := wrapped(context.Background(), connect.Spec{})

	got := []string{
		stream1.RequestHeader().Get("Authorization"),
		stream2.RequestHeader().Get("Authorization"),
	}
	want := []string{
		basicAuthHeader("watcher", "first"),
		basicAuthHeader("watcher", "rotated"),
	}

	if calls != len(credentials) {
		t.Fatalf("credentials provider calls = %d, want %d", calls, len(credentials))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("stream header %d = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestClientInterceptorsIncludeCredentialsWithoutRetry(t *testing.T) {
	t.Parallel()

	interceptors := clientInterceptors(Config{
		RetryOptions: NoRetry,
		Credentials: func() (string, string) {
			return "user", "pass"
		},
	})

	if len(interceptors) != 1 {
		t.Fatalf("len(interceptors) = %d, want 1", len(interceptors))
	}
	if _, ok := interceptors[0].(*basicAuthInterceptor); !ok {
		t.Fatalf("interceptors[0] = %T, want *basicAuthInterceptor", interceptors[0])
	}
}

func TestClientInterceptorsOrderRetryBeforeCredentials(t *testing.T) {
	t.Parallel()

	interceptors := clientInterceptors(Config{
		RetryOptions: &RetryOptions{
			UnaryAttempts: 1,
		},
		Credentials: func() (string, string) {
			return "user", "pass"
		},
	})

	if len(interceptors) != 2 {
		t.Fatalf("len(interceptors) = %d, want 2", len(interceptors))
	}
	if _, ok := interceptors[0].(connect.UnaryInterceptorFunc); !ok {
		t.Fatalf("interceptors[0] = %T, want connect.UnaryInterceptorFunc", interceptors[0])
	}
	if _, ok := interceptors[1].(*basicAuthInterceptor); !ok {
		t.Fatalf("interceptors[1] = %T, want *basicAuthInterceptor", interceptors[1])
	}
}

func basicAuthHeader(username, password string) string {
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(username+":"+password))
}

type stubStreamingClientConn struct {
	requestHeader   http.Header
	responseHeader  http.Header
	responseTrailer http.Header
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
	return nil
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
