package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"

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
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	c := client.New(&client.Config{
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

// type zapLoggerAdapter struct {
// 	l *zap.Logger
// }

// func (a *zapLoggerAdapter) CheckDebug() bool {
// 	return a.l.Core().Enabled(zap.DebugLevel)
// }

// func (a *zapLoggerAdapter) CheckInfo() bool {
// 	return a.l.Core().Enabled(zap.InfoLevel)
// }

// func (a *zapLoggerAdapter) Info(msg string, pairs ...any) {
// 	fields := make([]zap.Field, 0, len(pairs))
// 	for i := 0; i < len(pairs); i += 2 {
// 		fields = append(fields, zap.Any(pairs[i].(string), pairs[i+1]))
// 	}
// 	a.l.Info(msg, fields...)
// }

// func (a *zapLoggerAdapter) Debug(msg string, pairs ...any) {
// 	fields := make([]zap.Field, 0, len(pairs))
// 	for i := 0; i < len(pairs); i += 2 {
// 		fields = append(fields, zap.Any(pairs[i].(string), pairs[i+1]))
// 	}
// 	a.l.Debug(msg, fields...)
// }

// func (a *zapLoggerAdapter) Warn(msg string, pairs ...any) {
// 	fields := make([]zap.Field, 0, len(pairs))
// 	for i := 0; i < len(pairs); i += 2 {
// 		fields = append(fields, zap.Any(pairs[i].(string), pairs[i+1]))
// 	}
// 	a.l.Warn(msg, fields...)
// }
