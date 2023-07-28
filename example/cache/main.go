package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"

	"go.uber.org/zap"
	client "go.withmatt.com/connect-etcd"
	"go.withmatt.com/connect-etcd/cache"
	"go.withmatt.com/connect-etcd/types/mvccpb"
)

var (
	flagPrefix   = flag.String("prefix", "/", "")
	flagPageSize = flag.Int64("page-size", 100, "")
)

func init() { flag.Parse() }

func main() {
	ll := zap.NewExample()
	defer ll.Sync()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	c := client.New(&client.Config{
		Logger:   ll.With(zap.String("logger", "etcd")),
		Endpoint: "127.0.0.1:2379",
	})

	i := cache.Informer{
		Client:       c,
		Key:          []byte(*flagPrefix),
		RangeEnd:     client.NextKey([]byte(*flagPrefix)),
		ListPageSize: *flagPageSize,
		// RetryInterval: 1 * time.Second,

		AddFunc: func(kv *mvccpb.KeyValue) {
			fmt.Println("AddFunc", string(kv.Key))
		},
		UpdateFunc: func(kv *mvccpb.KeyValue) {
			fmt.Println("UpdateFunc", string(kv.Key))
		},
		DeleteFunc: func(kv *mvccpb.KeyValue) {
			fmt.Println("DeleteFunc", string(kv.Key))
		},
	}
	if err := i.Run(ctx); err != nil {
		if ctxErr := ctx.Err(); ctxErr == nil {
			panic(err)
		}
	}
}
