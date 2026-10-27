package cache

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	etcd "go.withmatt.com/connect-etcd"
	"go.withmatt.com/connect-etcd/types/etcdserverpb"
	"go.withmatt.com/connect-etcd/types/etcdserverpb/etcdserverpbconnect"
)

// fakeWatchHandler stubs only Watch; the embedded nil interface panics if
// anything else is called.
type fakeWatchHandler struct {
	etcdserverpbconnect.WatchHandler
	watch func(context.Context, *connect.BidiStream[etcdserverpb.WatchRequest, etcdserverpb.WatchResponse]) error
}

func (f *fakeWatchHandler) Watch(ctx context.Context, stream *connect.BidiStream[etcdserverpb.WatchRequest, etcdserverpb.WatchResponse]) error {
	return f.watch(ctx, stream)
}

// TestInformerStreamCancelsWatch ensures that Informer.stream cleans up its
// Watch stream completely.
func TestInformerStreamCancelsWatch(t *testing.T) {
	t.Parallel()

	// release allows the test to clean up all of its resources, even if
	// the code under test fails to clean up its stream request.
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	etcdAddr, watchCanceled := startFakeWatchCancelingEtcd(t, release)

	cl := etcd.NewClient(etcd.Config{
		Endpoints: []string{etcdAddr},
	})
	inf := &Informer{Client: cl, Key: []byte("test-key")}

	// Make sure the test doesn't take forever.
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	// Informer.stream should quickly return the cancelation error.
	const wantErr = `informer: stream canceled: ""`
	if err := inf.stream(ctx); err == nil || err.Error() != wantErr {
		t.Fatalf("stream() error = %v, want %q", err, wantErr)
	}

	// Now check that the server says we cleaned up.
	select {
	case <-watchCanceled:
		// All good!
	case <-ctx.Done():
		t.Fatalf("server-side watch RPC was not canceled after stream() returned (%v)", ctx.Err())
	}
}

// startFakeWatchCancelingEtcd creates a fake etcd server that acts like the
// requested watch is starting at a version that was compacted, so it will be
// canceled immediately. The server accepts the watch create, then cancels the
// established watch stream with no reason, like real etcd does.
//
// Returns the server address and a chan that signals when the stream is
// cleaned up.
func startFakeWatchCancelingEtcd(t *testing.T, release <-chan struct{}) (string, <-chan struct{}) {
	watchCanceled := make(chan struct{})

	path, handler := etcdserverpbconnect.NewWatchHandler(&fakeWatchHandler{
		watch: func(ctx context.Context, stream *connect.BidiStream[etcdserverpb.WatchRequest, etcdserverpb.WatchResponse]) error {
			req, err := stream.Receive()
			if err != nil {
				return err
			}
			watchId := req.GetCreateRequest().WatchId

			// Mimic etcd's behavior when a watch is started from a
			// revision that has already been compacted.

			// Tell the client that the watch is OK.
			if err := stream.Send(&etcdserverpb.WatchResponse{
				WatchId: watchId,
				Created: true,
				Header:  &etcdserverpb.ResponseHeader{Revision: 42},
			}); err != nil {
				return err
			}

			// Now, in place of any revisions, cancel the stream.
			if err := stream.Send(&etcdserverpb.WatchResponse{
				WatchId:  watchId,
				Canceled: true,
			}); err != nil {
				return err
			}

			// Make sure that the client closes the stream. If the
			// client doesn't do this, a real etcd server would
			// keep the request going and could run out of memory
			// if enough canceled streams accumlate.
			select {
			case <-ctx.Done():
				// Note: This will panic if more than one
				// request is made. The handler assumes only
				// one request.
				close(watchCanceled)
			case <-release:
			}
			return ctx.Err()
		},
	})

	mux := http.NewServeMux()
	mux.Handle(path, handler)
	srv := httptest.NewServer(h2c.NewHandler(mux, &http2.Server{}))

	t.Cleanup(func() {
		srv.Close()
	})

	return srv.Listener.Addr().String(), watchCanceled
}
