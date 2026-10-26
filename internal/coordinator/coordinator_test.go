package coordinator

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/pgatdesk/swarmflow/internal/bitfield"
	"github.com/pgatdesk/swarmflow/internal/conn"
	"github.com/pgatdesk/swarmflow/internal/dataset"
	"github.com/pgatdesk/swarmflow/internal/manifest"
	"github.com/pgatdesk/swarmflow/internal/protocol"
	"github.com/pgatdesk/swarmflow/internal/scheduler"
	"github.com/pgatdesk/swarmflow/internal/store"
)

// fakePeer is a scripted remote peer on the far end of an in-memory pipe, so
// tests control exactly which message the coordinator sees and when.
type fakePeer struct {
	t   *testing.T
	id  protocol.PeerID
	nc  net.Conn
	in  chan protocol.Message
	wmu sync.Mutex
}

func connectFake(t *testing.T, ctx context.Context, co *Coordinator, id byte, has bitfield.Bitfield) *fakePeer {
	t.Helper()
	ours, theirs := net.Pipe()
	fp := &fakePeer{t: t, id: protocol.PeerID{id}, nc: theirs, in: make(chan protocol.Message, 256)}
	go func() {
		for {
			m, err := protocol.ReadMessage(theirs)
			if err != nil {
				close(fp.in)
				return
			}
			fp.in <- m
		}
	}()
	c := conn.New(ours, fp.id, netip.MustParseAddrPort("127.0.0.1:1"), true, conn.DefaultConfig())
	co.AddConn(ctx, c)
	fp.send(protocol.Bitfield{Bits: has.Bytes()})
	t.Cleanup(func() { theirs.Close() })
	return fp
}

func (fp *fakePeer) send(m protocol.Message) {
	fp.wmu.Lock()
	defer fp.wmu.Unlock()
	if err := protocol.WriteMessage(fp.nc, m); err != nil {
		fp.t.Errorf("fake peer %x: send %s: %v", fp.id[0], m.Type(), err)
	}
}

// nextRequest returns the next REQUEST the coordinator sends this peer, or
// false if none arrives within d. Other message types are skipped.
func (fp *fakePeer) nextRequest(d time.Duration) (protocol.Request, bool) {
	timeout := time.After(d)
	for {
		select {
		case m, ok := <-fp.in:
			if !ok {
				return protocol.Request{}, false
			}
			if r, isReq := m.(protocol.Request); isReq {
				return r, true
			}
		case <-timeout:
			return protocol.Request{}, false
		}
	}
}

// haves collects HAVE announcements the coordinator sends this peer until
// none arrives for d.
func (fp *fakePeer) haves(d time.Duration) []int {
	var out []int
	for {
		select {
		case m, ok := <-fp.in:
			if !ok {
				return out
			}
			if h, isHave := m.(protocol.Have); isHave {
				out = append(out, int(h.Piece))
			}
		case <-time.After(d):
			return out
		}
	}
}

func TestDistributeRevealsDisjointWindowsAndRepeatsOnlyLosses(t *testing.T) {
	const pieceSize, pieces = 16 << 10, 24
	data := dataset.Bytes(pieces*pieceSize, 4)
	m, _ := manifest.Build(bytes.NewReader(data), "f.bin", int64(len(data)), pieceSize)
	src := filepath.Join(t.TempDir(), "f.bin")
	os.WriteFile(src, data, 0o644)
	st, err := store.OpenComplete(src, m)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	all := bitfield.New(pieces)
	for i := range pieces {
		all.Set(i)
	}
	cfg := DefaultConfig()
	cfg.Distribute = DistributeConfig{Enabled: true, Window: 4, AckTimeout: time.Hour}
	cfg.PingInterval, cfg.PEXInterval = time.Hour, 0
	co := New(m, st, all, protocol.PeerID{0xFF}, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- co.Run(ctx) }()
	defer func() { cancel(); <-done }()

	empty := bitfield.New(pieces)
	a := connectFake(t, ctx, co, 0x0A, empty)
	b := connectFake(t, ctx, co, 0x0B, empty)
	ga, gb := a.haves(300*time.Millisecond), b.haves(300*time.Millisecond)
	if len(ga) != 4 || len(gb) != 4 {
		t.Fatalf("revealed %v to a and %v to b; want windows of 4", ga, gb)
	}
	for _, i := range ga {
		if slices.Contains(gb, i) {
			t.Fatalf("piece %d revealed to both peers", i)
		}
	}
	// a acknowledges one piece: exactly one new, never-revealed piece follows.
	a.send(protocol.Have{Piece: uint32(ga[0])})
	more := a.haves(300 * time.Millisecond)
	if len(more) != 1 || slices.Contains(ga, more[0]) || slices.Contains(gb, more[0]) {
		t.Fatalf("after one ack a got %v", more)
	}
	// b leaves without acknowledging: its 4 pieces are the only ones repeated,
	// and they go out first once a has room in its window.
	b.nc.Close()
	if got := a.haves(300 * time.Millisecond); len(got) != 0 {
		t.Fatalf("a's window was full but it was given %v", got)
	}
	for _, i := range append(ga[1:], more...) {
		a.send(protocol.Have{Piece: uint32(i)})
	}
	again := a.haves(500 * time.Millisecond)
	if !slices.Equal(sortedCopy(again), sortedCopy(gb)) {
		t.Fatalf("after b left and a acknowledged its window, a was given %v; want b's unacknowledged %v first", again, gb)
	}
	// Once every piece has been acknowledged somewhere, everything is advertised.
	for i := range pieces {
		a.send(protocol.Have{Piece: uint32(i)}) // a now holds everything (duplicates are harmless)
	}
	waitUntil(t, 2*time.Second, func() bool {
		co.publishStatus(time.Now())
		d := co.Status().Distribution
		return d != nil && !d.Active && d.Acked == pieces && d.Repeated == 4
	})
}

func sortedCopy(xs []int) []int {
	out := slices.Clone(xs)
	slices.Sort(out)
	return out
}

func waitUntil(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	for end := time.Now().Add(d); time.Now().Before(end); time.Sleep(20 * time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatal("condition not reached")
}

// firstPeer always selects the first candidate (connection order).
type firstPeer struct{}

func (firstPeer) Name() string                                 { return "first" }
func (firstPeer) Select(_, _ int, _ []scheduler.Candidate) int { return 0 }

// blockingJournal holds the first persistence of a piece until released,
// keeping that piece in the Verifying state for as long as a test needs.
type blockingJournal struct {
	mu      sync.Mutex
	hold    map[int]chan struct{}
	entered chan int
}

func (j *blockingJournal) PiecePersisted(i int) error {
	j.mu.Lock()
	ch, ok := j.hold[i]
	delete(j.hold, i)
	j.mu.Unlock()
	if ok {
		j.entered <- i
		<-ch
	}
	return nil
}
func (j *blockingJournal) Completed() error                       { return nil }
func (j *blockingJournal) PeerSeen(string, protocol.PeerID) error { return nil }

// TestEndgameDoesNotReRequestVerifyingPiece is the regression test for a piece
// being persisted twice: endgame used to re-request a piece whose first copy
// was still being verified, resetting it to Requested, so the next copy was
// verified and persisted again (and a last piece "completed" twice).
func TestEndgameDoesNotReRequestVerifyingPiece(t *testing.T) {
	const pieceSize = 16 << 10
	data := dataset.Bytes(2*pieceSize, 1)
	m, err := manifest.Build(bytes.NewReader(data), "f.bin", int64(len(data)), pieceSize)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	st, _, err := store.Create(filepath.Join(dir, "f.part"), filepath.Join(dir, "f.bin"), m)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	piece := func(i int) []byte { return data[i*pieceSize : (i+1)*pieceSize] }

	release := make(chan struct{})
	j := &blockingJournal{hold: map[int]chan struct{}{0: release}, entered: make(chan int, 1)}
	cfg := DefaultConfig()
	cfg.Selector, cfg.Journal = firstPeer{}, j
	cfg.Endgame.MinAge, cfg.Endgame.Slack = 50*time.Millisecond, 1
	cfg.PingInterval, cfg.PEXInterval = time.Hour, time.Hour
	co := New(m, st, bitfield.New(2), protocol.PeerID{0xFF}, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- co.Run(ctx) }()
	defer func() { cancel(); <-done }()
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock() // runs first, so an early failure cannot leave a verify worker stuck

	all := bitfield.New(2)
	all.Set(0)
	all.Set(1)
	a := connectFake(t, ctx, co, 0x0A, all)
	reqA := map[uint32]protocol.Request{}
	for range 2 { // firstPeer sends both initial requests to a
		r, ok := a.nextRequest(2 * time.Second)
		if !ok {
			t.Fatal("no initial request to a")
		}
		reqA[r.Piece] = r
	}
	b := connectFake(t, ctx, co, 0x0B, all)
	reqB := map[uint32]protocol.Request{}
	for range 2 { // both are overdue at a, so endgame duplicates them to b
		r, ok := b.nextRequest(2 * time.Second)
		if !ok {
			t.Fatal("endgame did not duplicate requests to b")
		}
		reqB[r.Piece] = r
	}

	// Piece 1 completes normally from b; piece 0's first copy (from a) is held in Verifying.
	b.send(protocol.Piece{ReqID: reqB[1].ReqID, Piece: 1, Data: piece(1)})
	a.send(protocol.Piece{ReqID: reqA[0].ReqID, Piece: 0, Data: piece(0)})
	select {
	case <-j.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("piece 0 never reached persistence")
	}

	// Piece 0 now has one outstanding request (b's) and is overdue, but its
	// first copy is verifying: endgame must not request it again from a.
	for {
		r, ok := a.nextRequest(500 * time.Millisecond)
		if !ok {
			break
		}
		if r.Piece == 0 {
			t.Fatal("endgame re-requested a piece that is being verified")
		}
	}
	b.send(protocol.Piece{ReqID: reqB[0].ReqID, Piece: 0, Data: piece(0)}) // a late duplicate
	time.Sleep(100 * time.Millisecond)
	unblock()

	select {
	case <-co.Complete():
	case <-time.After(2 * time.Second):
		t.Fatal("download did not complete")
	}
	time.Sleep(200 * time.Millisecond) // let any second persistence surface
	co.publishStatus(time.Now())
	if s := co.Status(); s.Verifying != 0 || s.Counters.DuplicatePieces != 1 {
		t.Fatalf("verifying=%d duplicates=%d; want 0 and 1", s.Verifying, s.Counters.DuplicatePieces)
	}
}
