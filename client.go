package etcd

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"connectrpc.com/connect"
	"go.uber.org/zap"
	"golang.org/x/net/http2"

	"go.withmatt.com/connect-etcd/internal/codec"
	"go.withmatt.com/connect-etcd/internal/retry"
	"go.withmatt.com/connect-etcd/types/etcdserverpb/etcdserverpbconnect"
)

type Config struct {
	Endpoints    []string
	TLSConfig    *tls.Config
	Logger       *zap.Logger
	RetryOptions *RetryOptions
	DialContext  func(context.Context, string, string) (net.Conn, error)
}

type RetryOptions struct {
	UnaryAttempts int
	Interval      time.Duration
	Jitter        float64
}

var NoRetry = &RetryOptions{UnaryAttempts: 0}

type Client struct {
	httpClient connect.HTTPClient
	logger     *zap.Logger
	endpoint   string
	opts       []connect.ClientOption

	kvOnce   sync.Once
	kvClient etcdserverpbconnect.KVClient

	watchOnce   sync.Once
	watchClient etcdserverpbconnect.WatchClient

	leaseOnce   sync.Once
	leaseClient etcdserverpbconnect.LeaseClient
}

func (c *Client) Logger() *zap.Logger {
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

var defaultLogger = zap.NewNop()

func NewClient(cfg Config) *Client {
	protocol := "https"
	if cfg.TLSConfig == nil {
		protocol = "http"
	}
	if cfg.Logger == nil {
		cfg.Logger = defaultLogger
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
	opts := defaultClientOpts
	retryOpts := cfg.RetryOptions
	if retryOpts == nil {
		retryOpts = defaultRetryOptions
	}
	if retryOpts.UnaryAttempts == 0 {
		return opts
	}
	return append(opts, connect.WithInterceptors(retry.UnaryInterceptor(
		cfg.Logger,
		retryOpts.UnaryAttempts,
		retryOpts.Interval,
		retryOpts.Jitter,
	)))
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
