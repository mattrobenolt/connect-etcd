package cache

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"connectrpc.com/connect"
	"go.uber.org/zap"
	client "go.withmatt.com/connect-etcd"
	"go.withmatt.com/connect-etcd/internal/retry"
	"go.withmatt.com/connect-etcd/types/etcdserverpb"
	"go.withmatt.com/connect-etcd/types/mvccpb"
)

type Informer struct {
	Client   *client.Client
	Key      []byte
	RangeEnd []byte

	ListPageSize int64

	AddFunc    func(*mvccpb.KeyValue)
	UpdateFunc func(*mvccpb.KeyValue)
	DeleteFunc func(*mvccpb.KeyValue)

	cacheLock sync.RWMutex
	cache     map[string]*mvccpb.KeyValue

	lastRevision int64
	totalKeys    uint64

	syncDoneMu sync.Mutex
	syncDone   chan error
}

func nextKey(key []byte) []byte {
	// this is the key + NULL
	end := make([]byte, len(key)+1)
	copy(end, key)
	return end
}

func (i *Informer) LastRevision() int64 {
	return i.lastRevision
}

func (i *Informer) addFunc(kv *mvccpb.KeyValue) {
	i.totalKeys++
	if i.AddFunc != nil {
		i.AddFunc(kv)
	}
}

func (i *Informer) updateFunc(kv *mvccpb.KeyValue) {
	if i.UpdateFunc != nil {
		i.UpdateFunc(kv)
	}
}

func (i *Informer) deleteFunc(kv *mvccpb.KeyValue) {
	i.totalKeys--
	if i.DeleteFunc != nil {
		i.DeleteFunc(kv)
	}
}

func (i *Informer) Get(key []byte) (*mvccpb.KeyValue, bool) {
	i.cacheLock.RLock()
	defer i.cacheLock.RUnlock()

	kv, ok := i.cache[string(key)]
	return kv, ok
}

func (i *Informer) List() [][]byte {
	i.cacheLock.RLock()
	defer i.cacheLock.RUnlock()

	l := make([][]byte, 0, len(i.cache))
	for _, v := range i.cache {
		l = append(l, v.Key)
	}
	return l
}

const defaultPageSize = 100

func (i *Informer) load(ctx context.Context) error {
	i.cacheLock.Lock()

	l := i.Client.Logger()

	i.cache = make(map[string]*mvccpb.KeyValue)
	i.lastRevision = 0
	i.totalKeys = 0

	pageSize := i.ListPageSize
	if pageSize == 0 {
		pageSize = defaultPageSize
	}

	startKey := i.Key

	var limit int64
	var serializable bool

	for {
		if ce := l.Check(zap.DebugLevel, "Range"); ce != nil {
			ce.Write(
				zap.ByteString("key", startKey),
				zap.Int64("revision", i.lastRevision),
			)
		}

		// when lastRevision is 0, this is the first page we're requesting.
		// for the first page, we want to do a small strongly consistent read
		// (not serializable means linearizable read). We want to make a
		// smaller read against the primary node to ensure we have a good
		// starting state for pagination. Once we have a revision, we can
		// serve the rest of the pages from a replica with an explicit
		// revision.
		if i.lastRevision == 0 {
			if pageSize < defaultPageSize {
				limit = pageSize
			} else {
				limit = defaultPageSize
			}
			serializable = false
		} else {
			limit = pageSize
			serializable = true
		}

		resp, err := i.Client.KV().Range(ctx, connect.NewRequest(&etcdserverpb.RangeRequest{
			SortOrder:    etcdserverpb.RangeRequest_ASCEND,
			SortTarget:   etcdserverpb.RangeRequest_KEY,
			Key:          startKey,
			RangeEnd:     i.RangeEnd,
			Limit:        limit,
			Revision:     i.lastRevision,
			Serializable: serializable,
		}))
		if err != nil {
			i.cacheLock.Unlock()
			return err
		}
		msg := resp.Msg

		if i.lastRevision == 0 {
			i.lastRevision = msg.Header.Revision
		}

		for _, kv := range msg.Kvs {
			i.cache[string(kv.Key)] = kv
		}

		if !msg.More {
			break
		}

		startKey = nextKey(msg.Kvs[len(msg.Kvs)-1].Key)
	}

	i.cacheLock.Unlock()

	// send out the AddFunc calls after we've successfully synced
	for _, kv := range i.cache {
		i.addFunc(kv)
	}

	return nil
}

func (i *Informer) WaitForCacheSync(ctx context.Context) error {
	i.syncDoneMu.Lock()
	if i.syncDone == nil {
		i.syncDone = make(chan error)
	}
	i.syncDoneMu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-i.syncDone:
		return err
	}
}

func (i *Informer) Run(ctx context.Context) error {
	i.syncDoneMu.Lock()
	if i.syncDone == nil {
		i.syncDone = make(chan error)
	}
	i.syncDoneMu.Unlock()

	l := i.Client.Logger()

	start := time.Now()

	if err := i.load(ctx); err != nil {
		i.syncDone <- err
		close(i.syncDone)
		return err
	}

	if ce := l.Check(
		zap.InfoLevel,
		"informer sync finished",
	); ce != nil {
		ce.Write(
			zap.Uint64("total_keys", i.totalKeys),
			zap.Int64("revision", i.lastRevision),
			zap.Duration("duration", time.Since(start)),
		)
	}

	close(i.syncDone)

	// we explicitly want the stream to retry forever reconnecting
	// outside of the Unary interceptor retry intervals
	return retry.Forever(ctx, l,
		1*time.Second, 0.5,
		func() error { return i.stream(ctx) },
	)
}

func (i *Informer) stream(ctx context.Context) error {
	watchId := rand.Int63()
	l := i.Client.Logger().With(
		zap.Int64("watch_id", watchId),
	)

	stream := i.Client.Watch().Watch(ctx)
	defer stream.CloseRequest()

	if err := stream.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
			CreateRequest: &etcdserverpb.WatchCreateRequest{
				WatchId:       watchId,
				StartRevision: i.lastRevision + 1,
				Key:           i.Key,
				RangeEnd:      i.RangeEnd,

				ProgressNotify: true,
			},
		},
	}); err != nil {
		return err
	}

	msg, err := stream.Receive()
	if err != nil {
		return err
	}

	if msg.WatchId != watchId {
		return errors.New("informer: unexpected watch id, aborting stream")
	}

	if !msg.Created {
		return errors.New("informer: unexpected watch message, expected CreateResponse")
	}

	if ce := l.Check(zap.DebugLevel, "stream started"); ce != nil {
		ce.Write(
			zap.Uint64("cluster_id", msg.Header.ClusterId),
			zap.Uint64("member_id", msg.Header.MemberId),
			zap.Int64("revision", msg.Header.Revision),
			zap.Uint64("raft_term", msg.Header.RaftTerm),
		)
	}
	i.lastRevision = msg.Header.Revision

	errCh := make(chan error)

	go func() {
		defer close(errCh)

		for {
			msg, err := stream.Receive()
			if err != nil {
				errCh <- err
				return
			}

			if msg.WatchId != watchId {
				errCh <- errors.New("informer: unexpected watch id, aborting stream")
				return
			}

			if msg.Canceled {
				return
			}

			if ce := l.Check(zap.DebugLevel, "receive message"); ce != nil {
				ce.Write(
					zap.Uint64("cluster_id", msg.Header.ClusterId),
					zap.Uint64("member_id", msg.Header.MemberId),
					zap.Int64("revision", msg.Header.Revision),
					zap.Uint64("raft_term", msg.Header.RaftTerm),
					zap.Int64("last_revision", i.lastRevision),
					zap.Int("events", len(msg.Events)),
					zap.Bool("fragment", msg.Fragment),
				)
			}

			if msg.Header.Revision < i.lastRevision {
				errCh <- fmt.Errorf("informer: older revision observed: %d -> %d", i.lastRevision, msg.Header.Revision)
				return
			}

			for _, event := range msg.Events {
				i.cacheLock.Lock()
				switch event.Type {
				case mvccpb.Event_PUT:
					if _, ok := i.cache[string(event.Kv.Key)]; ok {
						i.updateFunc(event.Kv)
					} else {
						i.cache[string(event.Kv.Key)] = event.Kv
						i.addFunc(event.Kv)
					}
				case mvccpb.Event_DELETE:
					delete(i.cache, string(event.Kv.Key))
					i.deleteFunc(event.Kv)
				}
				i.cacheLock.Unlock()
			}

			i.lastRevision = msg.Header.Revision
		}
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}
