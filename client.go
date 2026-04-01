package etcd

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"connectrpc.com/connect"
	"golang.org/x/net/http2"

	"go.withmatt.com/connect-etcd/internal/codec"
	"go.withmatt.com/connect-etcd/internal/log"
	"go.withmatt.com/connect-etcd/internal/retry"
	"go.withmatt.com/connect-etcd/types/etcdserverpb/etcdserverpbconnect"
)

// CredentialsProvider returns the username and password for Basic Auth.
//
// It is called on each request, including each unary retry attempt and each
// new streaming RPC, so it may return different values over time to support
// credential rotation. Implementations must be safe for concurrent use.
type CredentialsProvider func() (username, password string)

type Config struct {
	Endpoints    []string
	TLSConfig    *tls.Config
	Logger       log.LeveledLogger
	RetryOptions *RetryOptions
	DialContext  func(context.Context, string, string) (net.Conn, error)
	Credentials  CredentialsProvider
}

type LeveledLogger = log.LeveledLogger

type slogAdapter struct {
	l *slog.Logger
}

func (a *slogAdapter) CheckDebug() bool {
	return a.l.Enabled(context.Background(), slog.LevelDebug)
}

func (a *slogAdapter) CheckInfo() bool {
	return a.l.Enabled(context.Background(), slog.LevelInfo)
}

func (a *slogAdapter) Info(msg string, args ...any) {
	a.l.Info(msg, args...)
}

func (a *slogAdapter) Debug(msg string, args ...any) {
	a.l.Debug(msg, args...)
}

func (a *slogAdapter) Warn(msg string, args ...any) {
	a.l.Warn(msg, args...)
}

type RetryOptions struct {
	UnaryAttempts int
	Interval      time.Duration
	Jitter        float64
}

var NoRetry = &RetryOptions{UnaryAttempts: 0}

type Client struct {
	httpClient connect.HTTPClient
	logger     LeveledLogger
	endpoint   string
	opts       []connect.ClientOption

	kvOnce   sync.Once
	kvClient etcdserverpbconnect.KVClient

	watchOnce   sync.Once
	watchClient etcdserverpbconnect.WatchClient

	leaseOnce   sync.Once
	leaseClient etcdserverpbconnect.LeaseClient
}

func (c *Client) Logger() LeveledLogger {
	return c.logger
}

func (c *Client) KV() etcdserverpbconnect.KVClient {
	c.kvOnce.Do(func() {
		c.kvClient = etcdserverpbconnect.NewKVClient(
			c.httpClient,
			c.endpoint,
			c.opts...,
		)
	})
	return c.kvClient
}

func (c *Client) Watch() etcdserverpbconnect.WatchClient {
	c.watchOnce.Do(func() {
		c.watchClient = etcdserverpbconnect.NewWatchClient(
			c.httpClient,
			c.endpoint,
			c.opts...,
		)
	})
	return c.watchClient
}

func (c *Client) Lease() etcdserverpbconnect.LeaseClient {
	c.leaseOnce.Do(func() {
		c.leaseClient = etcdserverpbconnect.NewLeaseClient(
			c.httpClient,
			c.endpoint,
			c.opts...,
		)
	})
	return c.leaseClient
}

func NewClient(cfg Config) *Client {
	protocol := "https"
	if cfg.TLSConfig == nil {
		protocol = "http"
	}
	if cfg.Logger == nil {
		cfg.Logger = log.Default
	}
	// TODO: handle multiple endpoints
	endpoint := protocol + "://" + cfg.Endpoints[0]
	return &Client{
		httpClient: defaultHTTPClient(cfg),
		logger:     cfg.Logger,
		endpoint:   endpoint,
		opts:       clientOptions(cfg),
	}
}

type simpleClient http2.Transport

func (c *simpleClient) Do(req *http.Request) (*http.Response, error) {
	return (*http2.Transport)(c).RoundTrip(req)
}

var defaultClientOpts = []connect.ClientOption{
	connect.WithGRPC(),
	connect.WithCodec(codec.DefaultCodec),
}

var defaultRetryOptions = &RetryOptions{
	UnaryAttempts: 5,
	Interval:      100 * time.Millisecond,
	Jitter:        0.2,
}

func clientOptions(cfg Config) []connect.ClientOption {
	opts := append([]connect.ClientOption{}, defaultClientOpts...)
	interceptors := clientInterceptors(cfg)
	if len(interceptors) == 0 {
		return opts
	}
	return append(opts, connect.WithInterceptors(interceptors...))
}

func clientInterceptors(cfg Config) []connect.Interceptor {
	retryOpts := cfg.RetryOptions
	if retryOpts == nil {
		retryOpts = defaultRetryOptions
	}
	interceptors := make([]connect.Interceptor, 0, 2)
	if retryOpts.UnaryAttempts > 0 {
		interceptors = append(interceptors, retry.UnaryInterceptor(
			cfg.Logger,
			retryOpts.UnaryAttempts,
			retryOpts.Interval,
			retryOpts.Jitter,
		))
	}
	if cfg.Credentials != nil {
		interceptors = append(interceptors, &basicAuthInterceptor{
			credentials: cfg.Credentials,
		})
	}
	return interceptors
}

// likely adopt go-upstream here
func defaultHTTPClient(cfg Config) connect.HTTPClient {
	tlsConfig := cfg.TLSConfig.Clone()
	if tlsConfig != nil {
		tlsConfig.NextProtos = []string{http2.NextProtoTLS}
	}
	transport := &http2.Transport{
		DisableCompression: true,
		TLSClientConfig:    tlsConfig,
	}
	dialContext := cfg.DialContext
	if dialContext == nil {
		var d net.Dialer
		dialContext = d.DialContext
	}
	if tlsConfig == nil {
		transport.AllowHTTP = true
		transport.DialTLSContext = func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
			return dialContext(ctx, network, addr)
		}
	} else {
		transport.DialTLSContext = func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
			rawConn, err := dialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			conn := tls.Client(rawConn, tlsConfig)
			if err := conn.HandshakeContext(ctx); err != nil {
				rawConn.Close()
				return nil, err
			}
			state := conn.ConnectionState()
			if p := state.NegotiatedProtocol; p != http2.NextProtoTLS {
				conn.Close()
				return nil, fmt.Errorf("upstream: unexpected ALPN protocol %q; want %q", p, http2.NextProtoTLS)
			}
			return conn, nil
		}
	}
	return (*simpleClient)(transport)
}

var (
	defaultTLSConfig     *tls.Config
	defaultTLSConfigOnce sync.Once
)

func DefaultTLSConfig() *tls.Config {
	defaultTLSConfigOnce.Do(func() {
		certPool, _ := x509.SystemCertPool()
		defaultTLSConfig = TLSConfigWithCertPool(certPool)
	})
	return defaultTLSConfig
}

func TLSConfigWithCertPool(roots *x509.CertPool) *tls.Config {
	return &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    roots,
	}
}

func TLSConfigWithRoot(cert string) (*tls.Config, error) {
	b, err := os.ReadFile(cert)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(b) {
		return nil, errors.New("no certificates found")
	}
	return TLSConfigWithCertPool(pool), nil
}

func NextKey(key []byte) []byte {
	end := make([]byte, len(key))
	copy(end, key)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i] = end[i] + 1
			end = end[:i+1]
			return end
		}
	}
	// next prefix does not exist (e.g., 0xffff);
	// default to WithFromKey policy
	return []byte{0}
}

type basicAuthInterceptor struct {
	credentials CredentialsProvider
}

func (i *basicAuthInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		username, password := i.credentials()
		setBasicAuthHeader(req.Header(), username, password)
		return next(ctx, req)
	}
}

func (i *basicAuthInterceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return func(ctx context.Context, spec connect.Spec) connect.StreamingClientConn {
		conn := next(ctx, spec)
		username, password := i.credentials()
		setBasicAuthHeader(conn.RequestHeader(), username, password)
		return conn
	}
}

func (i *basicAuthInterceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return next
}

func setBasicAuthHeader(header http.Header, username, password string) {
	auth := username + ":" + password
	header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(auth)))
}
