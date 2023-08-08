package client

import (
	"context"
	"testing"

	"connectrpc.com/connect"

	clientv3 "go.etcd.io/etcd/client/v3"
	etcd "go.withmatt.com/connect-etcd"
	"go.withmatt.com/connect-etcd/types/etcdserverpb"
)

func BenchmarkSimple(b *testing.B) {
	key := []byte("foo")
	value := []byte("bar")
	ctx := context.Background()

	oldClient, _ := clientv3.New(clientv3.Config{
		Endpoints: []string{"127.0.0.1:2379"},
	})

	newClient := etcd.NewClient(etcd.Config{
		Endpoints:    []string{"127.0.0.1:2379"},
		RetryOptions: etcd.NoRetry,
	}).KV()

	b.Run("Put-old", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			r, err := oldClient.Put(ctx, "foo", "bar")
			if err != nil {
				b.Fail()
			} else {
				if r.Header.Revision == 0 {
					b.Fail()
				}
			}
		}
	})

	b.Run("Put-new", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			r, err := newClient.Put(ctx, connect.NewRequest(&etcdserverpb.PutRequest{
				Key:   key,
				Value: value,
			}))
			if err != nil {
				b.Fail()
			} else {
				if r.Msg.Header.Revision == 0 {
					b.Fail()
				}
			}
		}
	})

	b.Run("Get-old", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			r, err := oldClient.Get(ctx, "foo")
			if err != nil {
				b.Fail()
			} else {
				if r.Count != 1 {
					b.Fail()
				}
			}
		}
	})

	b.Run("Get-new", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			r, err := newClient.Range(ctx, connect.NewRequest(&etcdserverpb.RangeRequest{
				Key: key,
			}))
			if err != nil {
				b.Fail()
			} else {
				if r.Msg.Count != 1 {
					b.Fail()
				}
			}
		}
	})
}
