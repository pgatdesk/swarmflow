package coordinator

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/pgatdesk/swarmflow/internal/conn"
	"github.com/pgatdesk/swarmflow/internal/protocol"
	"github.com/pgatdesk/swarmflow/internal/store"
)

// uploader serves one remote peer's requests from its own goroutine, so disk
// reads and a slow receiver only ever block that peer's uploads.
type uploader struct {
	c     *conn.Conn
	st    *store.Store
	q     chan protocol.Request
	slots int
	sent  *atomic.Int64
	// queued counts requests accepted but not yet finished across all of the
	// swarm's uploaders; the coordinator compares it with UploadQueueCap.
	queued *atomic.Int32
	log    *slog.Logger

	mu sync.Mutex
	// pending holds queued request IDs (value: cancelled). It is bounded by slots,
	// so a peer cannot grow it by sending CANCELs for arbitrary IDs.
	pending map[uint32]bool
	closed  bool
}

func newUploader(c *conn.Conn, st *store.Store, slots int, sent *atomic.Int64, queued *atomic.Int32, log *slog.Logger) *uploader {
	return &uploader{c: c, st: st, q: make(chan protocol.Request, slots), slots: slots, sent: sent, queued: queued,
		log: log, pending: make(map[uint32]bool)}
}

// enqueue reports false if all upload slots are taken, the ID is already
// queued, or the uploader has stopped.
func (u *uploader) enqueue(r protocol.Request) bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	if _, dup := u.pending[r.ReqID]; dup || u.closed || len(u.pending) >= u.slots {
		return false
	}
	select {
	case u.q <- r:
		u.pending[r.ReqID] = false
		u.queued.Add(1)
		return true
	default:
		return false
	}
}

func (u *uploader) cancel(id uint32) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if _, ok := u.pending[id]; ok {
		u.pending[id] = true
	}
}

func (u *uploader) run(ctx context.Context) {
	defer func() {
		// Release the swarm-wide queue slots of requests that will never be served.
		u.mu.Lock()
		u.closed = true
		u.queued.Add(-int32(len(u.pending)))
		clear(u.pending)
		u.mu.Unlock()
	}()
	for {
		var r protocol.Request
		select {
		case <-ctx.Done():
			return
		case r = <-u.q:
		}
		u.mu.Lock()
		cancelled, ok := u.pending[r.ReqID]
		delete(u.pending, r.ReqID)
		u.mu.Unlock()
		if !ok {
			continue // released by a previous exit path; cannot happen while running
		}
		if !u.serve(ctx, r, cancelled) {
			return
		}
	}
}

// serve handles one dequeued request and reports whether the uploader should continue.
func (u *uploader) serve(ctx context.Context, r protocol.Request, cancelled bool) bool {
	defer u.queued.Add(-1)
	if cancelled {
		return true
	}
	data, err := u.st.ReadPiece(int(r.Piece))
	if err != nil {
		u.log.Error("upload read failed", "piece", r.Piece, "err", err)
		u.c.SendControl(protocol.Reject{ReqID: r.ReqID, Reason: protocol.RejectNotHave})
		return true
	}
	if err := u.c.SendData(ctx, protocol.Piece{ReqID: r.ReqID, Piece: r.Piece, Data: data}); err != nil {
		return false
	}
	u.sent.Add(int64(len(data)))
	return true
}
