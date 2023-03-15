package client

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"net/http"
	"os"
	"sync"

	"github.com/bufbuild/connect-go"
	"golang.org/x/net/http2"

	"go.withmatt.com/connect-etcd/internal/codec"
	"go.withmatt.com/connect-etcd/types/etcdserverpb/etcdserverpbconnect"
)

type Config struct {
	Endpoint  string
	TLSConfig *tls.Config
}

type Client struct {
	httpClient connect.HTTPClient
	endpoint   string
	opts       []connect.ClientOption

	kvOnce   sync.Once
	kvClient etcdserverpbconnect.KVClient

	watchOnce   sync.Once
	watchClient etcdserverpbconnect.WatchClient

	leaseOnce   sync.Once
	leaseClient etcdserverpbconnect.LeaseClient
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

func New(cfg *Config) *Client {
	protocol := "https"
	if cfg.TLSConfig == nil {
		protocol = "http"
	}
	return &Client{
		httpClient: defaultHTTPClient(cfg),
		endpoint:   protocol + "://" + cfg.Endpoint,
		opts:       defaultClientOptions(),
	}
}

type simpleClient struct {
	http.RoundTripper
}

func (c *simpleClient) Do(req *http.Request) (*http.Response, error) {
	return c.RoundTripper.RoundTrip(req)
}

var (
	defaultClientOpts     []connect.ClientOption
	defaultClientOptsOnce sync.Once
)

func defaultClientOptions() []connect.ClientOption {
	defaultClientOptsOnce.Do(func() {
		defaultClientOpts = []connect.ClientOption{
			connect.WithGRPC(),
			connect.WithCodec(codec.DefaultCodec),
		}
	})
	return defaultClientOpts
}

func defaultHTTPClient(cfg *Config) connect.HTTPClient {
	tlsConfig := cfg.TLSConfig
	transport := &http2.Transport{
		DisableCompression: true,
		TLSClientConfig:    tlsConfig,
	}
	if tlsConfig == nil {
		transport.AllowHTTP = true
		transport.DialTLSContext = func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, network, addr)
		}
	}
	return &simpleClient{transport}
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
