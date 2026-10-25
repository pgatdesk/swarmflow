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
	log   *slog.Logger

	mu sync.Mutex
	// pending holds queued request IDs (value: cancelled). It is bounded by slots,
	// so a peer cannot grow it by sending CANCELs for arbitrary IDs.
	pending map[uint32]bool
}

func newUploader(c *conn.Conn, st *store.Store, slots int, sent *atomic.Int64, log *slog.Logger) *uploader {
	return &uploader{c: c, st: st, q: make(chan protocol.Request, slots), slots: slots, sent: sent, log: log,
		pending: make(map[uint32]bool)}
}

// enqueue reports false if all upload slots are taken or the ID is already queued.
func (u *uploader) enqueue(r protocol.Request) bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	if _, dup := u.pending[r.ReqID]; dup || len(u.pending) >= u.slots {
		return false
	}
	select {
	case u.q <- r:
		u.pending[r.ReqID] = false
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
	for {
		var r protocol.Request
		select {
		case <-ctx.Done():
			return
		case r = <-u.q:
		}
		u.mu.Lock()
		cancelled := u.pending[r.ReqID]
		delete(u.pending, r.ReqID)
		u.mu.Unlock()
		if cancelled {
			continue
		}
		data, err := u.st.ReadPiece(int(r.Piece))
		if err != nil {
			u.log.Error("upload read failed", "piece", r.Piece, "err", err)
			u.c.SendControl(protocol.Reject{ReqID: r.ReqID, Reason: protocol.RejectNotHave})
			continue
		}
		if err := u.c.SendData(ctx, protocol.Piece{ReqID: r.ReqID, Piece: r.Piece, Data: data}); err != nil {
			return
		}
		u.sent.Add(int64(len(data)))
	}
}
