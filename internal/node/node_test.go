package node

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/pgatdesk/swarmflow/internal/bitfield"
	"github.com/pgatdesk/swarmflow/internal/coordinator"
	"github.com/pgatdesk/swarmflow/internal/dataset"
	"github.com/pgatdesk/swarmflow/internal/discovery"
	"github.com/pgatdesk/swarmflow/internal/manifest"
	"github.com/pgatdesk/swarmflow/internal/protocol"
	"github.com/pgatdesk/swarmflow/internal/store"
)

const pieceSize = 16 << 10

func testLogger(t *testing.T) *slog.Logger {
	if testing.Verbose() {
		return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	}
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

type fixture struct {
	t    *testing.T
	dir  string
	data []byte
	m    *manifest.Manifest
}

func newFixture(t *testing.T, size int) *fixture {
	data := dataset.Bytes(size, 42)
	m, err := manifest.Build(bytes.NewReader(data), "payload.bin", int64(size), pieceSize)
	if err != nil {
		t.Fatal(err)
	}
	return &fixture{t: t, dir: t.TempDir(), data: data, m: m}
}

// writeSource writes the payload to a file, keeping only pieces for which keep
// returns true (the rest are zeroed), and returns its path.
func (f *fixture) writeSource(name string, keep func(i int) bool) string {
	b := bytes.Clone(f.data)
	for i := range f.m.NumPieces() {
		if keep != nil && !keep(i) {
			off := f.m.PieceOffset(i)
			clear(b[off : off+int64(f.m.PieceLen(i))])
		}
	}
	p := filepath.Join(f.dir, name)
	if err := os.WriteFile(p, b, 0o644); err != nil {
		f.t.Fatal(err)
	}
	return p
}

type testPeer struct {
	n      *Node
	co     *coordinator.Coordinator
	st     *store.Store
	out    string
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

type peerOpts struct {
	seedFile string // seed this file; otherwise download
	listener net.Listener
	tracker  string
	peers    []string
	ccfg     func(*coordinator.Config)
}

func (f *fixture) start(name string, o peerOpts) *testPeer {
	t := f.t
	var (
		st   *store.Store
		have bitfield.Bitfield
		err  error
		out  = filepath.Join(f.dir, name, "payload.bin")
	)
	if o.seedFile != "" {
		if st, err = store.OpenComplete(o.seedFile, f.m); err != nil {
			t.Fatal(err)
		}
		if have, err = st.Recheck(context.Background(), 2); err != nil {
			t.Fatal(err)
		}
	} else {
		os.MkdirAll(filepath.Dir(out), 0o755)
		if st, _, err = store.Create(out+".part", out, f.m); err != nil {
			t.Fatal(err)
		}
		have = bitfield.New(f.m.NumPieces())
	}
	ncfg := DefaultConfig()
	ncfg.Listener, ncfg.TrackerURL, ncfg.StaticPeers = o.listener, o.tracker, o.peers
	ncfg.AnnounceInterval, ncfg.LonelyInterval = time.Second, 200*time.Millisecond
	ncfg.MinBackoff, ncfg.MaxBackoff = 100*time.Millisecond, time.Second
	n, err := New(ncfg, testLogger(t).With("name", name))
	if err != nil {
		t.Fatal(err)
	}
	ccfg := coordinator.DefaultConfig()
	ccfg.RequestTimeout = 2 * time.Second
	if o.ccfg != nil {
		o.ccfg(&ccfg)
	}
	co := coordinator.New(f.m, st, have, n.ID(), ccfg, n.Logger())
	n.AddSwarm(co)
	ctx, cancel := context.WithCancel(context.Background())
	p := &testPeer{n: n, co: co, st: st, out: out, cancel: cancel}
	p.wg.Go(func() { co.Run(ctx) })
	p.wg.Go(func() { n.Run(ctx) })
	t.Cleanup(p.stop)
	return p
}

func (p *testPeer) addr() string { return p.n.Addr().String() }

func (p *testPeer) stop() {
	p.cancel()
	p.wg.Wait()
	p.st.Close()
}

func (f *fixture) waitComplete(timeout time.Duration, peers ...*testPeer) {
	f.t.Helper()
	deadline := time.After(timeout)
	for _, p := range peers {
		select {
		case <-p.co.Complete():
		case <-deadline:
			s := p.co.Status()
			f.t.Fatalf("timed out with %d/%d pieces", s.Have, s.Pieces)
		}
	}
}

func (f *fixture) checkOutput(p *testPeer) {
	f.t.Helper()
	got, err := os.ReadFile(p.out)
	if err != nil {
		f.t.Fatal(err)
	}
	if !bytes.Equal(got, f.data) {
		f.t.Fatal("downloaded file differs from source")
	}
	if _, err := os.Stat(p.out + ".part"); !os.IsNotExist(err) {
		f.t.Fatal(".part file left behind")
	}
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	for end := time.Now().Add(timeout); time.Now().Before(end); time.Sleep(20 * time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatal("condition not reached")
}

func startTracker(t *testing.T) string {
	tr := discovery.NewTracker(testLogger(t))
	srv := httptest.NewServer(tr.Handler())
	t.Cleanup(srv.Close)
	return srv.URL
}

func TestOneSeederThreeDownloadersViaTracker(t *testing.T) {
	f := newFixture(t, 2<<20+1234)
	tr := startTracker(t)
	f.start("seed", peerOpts{seedFile: f.writeSource("src.bin", nil), tracker: tr})
	var ds []*testPeer
	for _, name := range []string{"d1", "d2", "d3"} {
		ds = append(ds, f.start(name, peerOpts{tracker: tr}))
	}
	f.waitComplete(30*time.Second, ds...)
	for _, d := range ds {
		f.checkOutput(d)
	}
}

func TestPartialSeedersCoverFile(t *testing.T) {
	f := newFixture(t, 1<<20)
	even := f.start("even", peerOpts{seedFile: f.writeSource("even.bin", func(i int) bool { return i%2 == 0 })})
	odd := f.start("odd", peerOpts{seedFile: f.writeSource("odd.bin", func(i int) bool { return i%2 == 1 })})
	d := f.start("d", peerOpts{peers: []string{even.addr(), odd.addr()}})
	f.waitComplete(30*time.Second, d)
	f.checkOutput(d)
	// Seed mode is upload-only: partial seeders must not fetch from the downloader.
	for _, s := range []*testPeer{even, odd} {
		if down := s.co.Status().BytesDown; down != 0 {
			t.Errorf("partial seeder downloaded %d bytes", down)
		}
	}
}

func TestSeederKilledMidTransfer(t *testing.T) {
	f := newFixture(t, 2<<20)
	src := f.writeSource("src.bin", nil)
	ln := newImpairedListener(t, impairment{bytesPerSec: 256 << 10})
	a := f.start("a", peerOpts{seedFile: src, listener: ln})
	b := f.start("b", peerOpts{seedFile: src, listener: newImpairedListener(t, impairment{bytesPerSec: 256 << 10})})
	d := f.start("d", peerOpts{peers: []string{a.addr(), b.addr()}})

	waitFor(t, 20*time.Second, func() bool { return d.co.Status().Have >= d.co.Status().Pieces/5 })
	before := d.co.Status().Have
	ln.killAll() // abrupt: sockets reset, no BYE
	a.stop()
	t.Logf("killed seeder a at %d/%d pieces", before, d.co.Status().Pieces)

	f.waitComplete(40*time.Second, d)
	f.checkOutput(d)
}

func TestStalledPeerDoesNotBlockProgress(t *testing.T) {
	f := newFixture(t, 1<<20)
	src := f.writeSource("src.bin", nil)
	// The stalled seeder completes the handshake and BITFIELD, then never sends again.
	stalled := f.start("stalled", peerOpts{seedFile: src, listener: newImpairedListener(t, impairment{stallAfter: 1024})})
	good := f.start("good", peerOpts{seedFile: src, listener: newImpairedListener(t, impairment{bytesPerSec: 1 << 20})})
	d := f.start("d", peerOpts{
		peers: []string{stalled.addr(), good.addr()},
		ccfg:  func(c *coordinator.Config) { c.RequestTimeout = 500 * time.Millisecond },
	})
	f.waitComplete(30*time.Second, d)
	f.checkOutput(d)
	timeouts := 0
	for _, p := range d.co.Status().Peers {
		timeouts += p.Timeouts
	}
	if timeouts == 0 {
		t.Fatal("expected requests to the stalled peer to time out")
	}
}

func TestCorruptedPieceIsRejected(t *testing.T) {
	f := newFixture(t, 512<<10)
	src := f.writeSource("src.bin", nil)
	// Flip one byte well past the handshake and BITFIELD, inside some PIECE payload.
	bad := f.start("bad", peerOpts{seedFile: src, listener: newImpairedListener(t, impairment{corruptAt: 100 << 10})})
	good := f.start("good", peerOpts{seedFile: src})
	d := f.start("d", peerOpts{peers: []string{bad.addr(), good.addr()}})
	f.waitComplete(30*time.Second, d)
	f.checkOutput(d)
	if r := d.co.Status().Rejected; r < 1 {
		t.Fatalf("rejected pieces = %d, want ≥ 1", r)
	}
}

func TestHandshakeRejections(t *testing.T) {
	f := newFixture(t, 64<<10)
	s := f.start("s", peerOpts{seedFile: f.writeSource("src.bin", nil)})

	try := func(h protocol.Handshake) protocol.ByeReason {
		t.Helper()
		nc, err := net.Dial("tcp", s.addr())
		if err != nil {
			t.Fatal(err)
		}
		defer nc.Close()
		nc.SetDeadline(time.Now().Add(5 * time.Second))
		protocol.WriteHandshake(nc, h)
		theirs, err := protocol.ReadHandshake(nc)
		if err != nil {
			t.Fatal(err)
		}
		if theirs.Swarm != (protocol.SwarmID{}) {
			t.Fatal("rejecting handshake should carry a zero swarm ID")
		}
		m, err := protocol.ReadMessage(nc)
		if err != nil {
			t.Fatal(err)
		}
		return m.(protocol.Bye).Reason
	}
	if r := try(protocol.Handshake{Major: 2, Swarm: f.m.ID(), Peer: protocol.PeerID{1}}); r != protocol.ByeIncompatibleVersion {
		t.Errorf("version mismatch: got %s", r)
	}
	if r := try(protocol.Handshake{Major: 1, Swarm: protocol.SwarmID{9}, Peer: protocol.PeerID{1}}); r != protocol.ByeUnknownSwarm {
		t.Errorf("unknown swarm: got %s", r)
	}
	if r := try(protocol.Handshake{Major: 1, Swarm: f.m.ID(), Peer: s.n.ID()}); r != protocol.ByeSelf {
		t.Errorf("self: got %s", r)
	}
}

func TestDownloaderFindsSeederThatStartsLater(t *testing.T) {
	f := newFixture(t, 256<<10)
	tr := startTracker(t)
	d := f.start("d", peerOpts{tracker: tr})
	time.Sleep(500 * time.Millisecond) // several empty announces
	f.start("seed", peerOpts{seedFile: f.writeSource("src.bin", nil), tracker: tr})
	f.waitComplete(20*time.Second, d)
	f.checkOutput(d)
}
