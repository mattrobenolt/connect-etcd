package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"connectrpc.com/connect"
	client "go.withmatt.com/connect-etcd"
	etcd "go.withmatt.com/connect-etcd/types/etcdserverpb"
)

var (
	flagTLSCA   = flag.String("tls-ca", "", "CA cert for TLS config")
	flagTLSCert = flag.String("tls-cert", "", "Cert for TLS config")
	flagTLSKey  = flag.String("tls-key", "", "Key for TLS config")
)

func main() {
	flag.Parse()

	var tlsConfig *tls.Config
	if *flagTLSCA != "" && *flagTLSCert != "" && *flagTLSKey != "" {
		var err error
		tlsConfig, err = mTLSConfig(
			*flagTLSCA,
			*flagTLSCert,
			*flagTLSKey,
		)
		if err != nil {
			panic(err)
		}
	}
	c := client.New(&client.Config{
		Endpoints: "127.0.0.1:2379",
		TLSConfig: tlsConfig,
	})

	// WaitGroup to know once we've gotten a response from the server
	// that our watch has been established.
	var wg sync.WaitGroup

	// done channel to signal when the watcher is finished and closed
	done := make(chan struct{})

	// first create our watch stream
	// r := client.Retryer(c.Watch()).Watch(context.Background())
	stream := client.Retryer(context.TODO(), c.Watch()).Watch(context.Background())
	go func() {
		defer func() {
			stream.Close()
			// stream.CloseRequest()
			// stream.CloseResponse()
			close(done)
		}()

		// the first message we get back is a response
		// to the WatchCreateRequest we will send
		msg, err := stream.Receive()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return
			}
		}
		// if somehow this first message isn't a Created, I dunno, it's broken
		if !msg.Created {
			panic("unexpected watch msg")
		}

		// signal that we've established our watch
		wg.Done()

		// begin looping for normal events
		for {
			msg, err := stream.Receive()
			if err != nil {
				if errors.Is(err, io.EOF) {
					done <- struct{}{}
					return
				}
				panic(err)
			}
			// if we got a cancel request, we are done here
			if msg.Canceled {
				done <- struct{}{}
				return
			}

			fmt.Println("rev:", msg.Header.Revision)
			if msg.Events != nil {
				for _, e := range msg.Events {
					fmt.Println("event", e)
				}
			} else {
				fmt.Println("stream", msg, err)
			}
		}
	}()

	// establish our watch stream with a WatchCreateRequest
	// and watch all of `/foo/` prefix
	wg.Add(1)
	if err := stream.Send(&etcd.WatchRequest{
		RequestUnion: &etcd.WatchRequest_CreateRequest{
			CreateRequest: &etcd.WatchCreateRequest{
				Key:      []byte("/foo/"),
				RangeEnd: client.NextKey([]byte("/foo/")),
			},
		},
	}); err != nil {
		panic(err)
	}

	// wait patiently for the watch stream to receive the Created response
	wg.Wait()

	// write a key
	r1, err := c.KV().Put(context.Background(), connect.NewRequest(&etcd.PutRequest{
		Key:    []byte("/foo/bar"),
		Value:  []byte("bar"),
		PrevKv: true,
	}))
	if err != nil {
		panic(err)
	}

	fmt.Println(r1.Header())
	fmt.Println(r1.Msg.PrevKv)

	// read a single key, may return 0 or more `Kvs` since this is a range request.
	// In practice, since we aren't sending `RangeEnd`, this will either be 0 or 1 Kvs.
	r, err := c.KV().Range(context.Background(), connect.NewRequest(&etcd.RangeRequest{
		Key: []byte("/foo/bar"),
	}))
	if err != nil {
		panic(err)
	}
	fmt.Println(r.Msg.Count, r.Msg.Kvs)

	key := r.Msg.Kvs[0]
	fmt.Println(key)

	r2, err := c.KV().Txn(context.Background(), connect.NewRequest(&etcd.TxnRequest{
		Compare: []*etcd.Compare{
			{
				Key:    []byte("/foo/bar"),
				Result: etcd.Compare_EQUAL,
				Target: etcd.Compare_CREATE,
				TargetUnion: &etcd.Compare_Version{
					Version: 0,
				},
			},
		},
		Success: []*etcd.RequestOp{
			{
				Request: &etcd.RequestOp_RequestPut{
					RequestPut: &etcd.PutRequest{
						Key:   []byte("/foo/bar"),
						Value: []byte("nope"),
					},
				},
			},
		},
	}))
	if err != nil {
		panic(err)
	}
	fmt.Println(r2.Msg.Succeeded)

	r3, err := c.KV().Txn(context.Background(), connect.NewRequest(&etcd.TxnRequest{
		Compare: []*etcd.Compare{
			{
				Key:    []byte("/foo/bar"),
				Result: etcd.Compare_EQUAL,
				Target: etcd.Compare_MOD,
				TargetUnion: &etcd.Compare_ModRevision{
					ModRevision: key.ModRevision,
				},
			},
		},
		Success: []*etcd.RequestOp{
			{
				Request: &etcd.RequestOp_RequestPut{
					RequestPut: &etcd.PutRequest{
						Key:   []byte("/foo/bar"),
						Value: []byte("nope"),
					},
				},
			},
		},
	}))
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(r3.Msg.Succeeded)

	r4, err := c.Lease().LeaseGrant(context.Background(), connect.NewRequest(&etcd.LeaseGrantRequest{
		TTL: 3,
	}))
	if err != nil {
		panic(err)
	}
	lease := r4.Msg
	fmt.Println(lease.ID, lease.TTL)

	r5, err := c.KV().Txn(context.Background(), connect.NewRequest(&etcd.TxnRequest{
		Compare: []*etcd.Compare{
			{
				Key:    []byte("/foo/expiring"),
				Result: etcd.Compare_EQUAL,
				Target: etcd.Compare_VERSION,
				TargetUnion: &etcd.Compare_ModRevision{
					ModRevision: 0,
				},
			},
		},
		Success: []*etcd.RequestOp{
			{
				Request: &etcd.RequestOp_RequestPut{
					RequestPut: &etcd.PutRequest{
						Key:   []byte("/foo/expiring"),
						Value: []byte("nope"),
						Lease: lease.ID,
					},
				},
			},
		},
	}))
	if err != nil {
		panic(err)
	}
	fmt.Println(r5.Msg)

	time.Sleep(4 * time.Second)

	// close down the watch stream
	stream.Send(&etcd.WatchRequest{
		RequestUnion: &etcd.WatchRequest_CancelRequest{
			CancelRequest: &etcd.WatchCancelRequest{},
		},
	})

	// wait patiently for it to close cleanly
	<-done
}

func mTLSConfig(ca, cert, key string) (*tls.Config, error) {
	caContents, err := os.ReadFile(ca)
	if err != nil {
		return nil, err
	}
	root := x509.NewCertPool()
	if !root.AppendCertsFromPEM(caContents) {
		return nil, errors.New("invalid CA cert")
	}
	leaf, err := tls.LoadX509KeyPair(cert, key)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		MinVersion:   tls.VersionTLS12,
		RootCAs:      root,
		Certificates: []tls.Certificate{leaf},
	}, nil
}
