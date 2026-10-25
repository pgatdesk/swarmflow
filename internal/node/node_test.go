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
	"github.com/pgatdesk/swarmflow/internal/faultnet"
	"github.com/pgatdesk/swarmflow/internal/manifest"
	"github.com/pgatdesk/swarmflow/internal/netx"
	"github.com/pgatdesk/swarmflow/internal/protocol"
	"github.com/pgatdesk/swarmflow/internal/scheduler"
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
	// claimAll makes a seeder advertise every piece without checking its file,
	// so it serves whatever bytes are on disk (a lying or corrupted peer).
	claimAll bool
	// acceptor runs a replica-only peer: it accepts REPL_OFFERs and fetches
	// only the pieces it accepted.
	acceptor bool
	// replicas is the target copy count this peer maintains (0 = off).
	replicas int
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
		if o.claimAll {
			for i := range f.m.NumPieces() {
				have.Set(i)
			}
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
	ncfg.AcceptReplicas = o.acceptor
	ncfg.MinBackoff, ncfg.MaxBackoff = 100*time.Millisecond, time.Second
	n, err := New(ncfg, testLogger(t).With("name", name))
	if err != nil {
		st.Close()
		t.Fatal(err)
	}
	ccfg := coordinator.DefaultConfig()
	ccfg.RequestTimeout = 2 * time.Second
	// PEX makes peers dial each other directly, bypassing the impaired
	// listeners these tests rely on; only the PEX test enables it.
	ccfg.PEXInterval = 0
	ccfg.ReplicaOnly, ccfg.Replication.Accept = o.acceptor, o.acceptor
	ccfg.Replication.Factor, ccfg.Replication.Interval = o.replicas, 50*time.Millisecond
	id := n.ID()
	seed := uint64(id[0])<<8 | uint64(id[1])
	ccfg.Picker = scheduler.NewRarestFirst(seed)
	ccfg.Selector = scheduler.NewAdaptivePeer(seed, scheduler.DefaultExplore)
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

func impaired(t *testing.T, imp faultnet.Impairment) *faultnet.Listener {
	ln, err := faultnet.Listen("127.0.0.1:0", imp)
	if err != nil {
		t.Fatal(err)
	}
	return ln
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
	ln := impaired(t, faultnet.Impairment{BytesPerSec: 256 << 10})
	a := f.start("a", peerOpts{seedFile: src, listener: ln})
	b := f.start("b", peerOpts{seedFile: src, listener: impaired(t, faultnet.Impairment{BytesPerSec: 256 << 10})})
	d := f.start("d", peerOpts{peers: []string{a.addr(), b.addr()}})

	waitFor(t, 20*time.Second, func() bool { return d.co.Status().Have >= d.co.Status().Pieces/5 })
	before := d.co.Status().Have
	ln.KillAll() // abrupt: sockets reset, no BYE
	a.stop()
	t.Logf("killed seeder a at %d/%d pieces", before, d.co.Status().Pieces)

	f.waitComplete(40*time.Second, d)
	f.checkOutput(d)
}

func TestStalledPeerDoesNotBlockProgress(t *testing.T) {
	f := newFixture(t, 1<<20)
	src := f.writeSource("src.bin", nil)
	// The stalled seeder completes the handshake and BITFIELD, then never sends again.
	stalled := f.start("stalled", peerOpts{seedFile: src, listener: impaired(t, faultnet.Impairment{StallAfter: 1024})})
	good := f.start("good", peerOpts{seedFile: src, listener: impaired(t, faultnet.Impairment{BytesPerSec: 1 << 20})})
	d := f.start("d", peerOpts{
		peers: []string{stalled.addr(), good.addr()},
		ccfg:  func(c *coordinator.Config) { c.RequestTimeout = 500 * time.Millisecond },
	})
	f.waitComplete(30*time.Second, d)
	f.checkOutput(d)
	var stalledStatus *coordinator.PeerStatus
	for _, p := range d.co.Status().Peers {
		if p.ID == stalled.n.ID().Short() {
			stalledStatus = &p
		}
	}
	if stalledStatus == nil {
		t.Fatal("stalled peer disconnected; it should be deprioritised, not dropped")
	}
	if stalledStatus.Timeouts < 3 || !stalledStatus.Cooldown {
		t.Fatalf("stalled peer: %d timeouts, cooldown=%v; want ≥3 and in cooldown", stalledStatus.Timeouts, stalledStatus.Cooldown)
	}
}

func TestEndgameRescuesPiecesFromSlowPeer(t *testing.T) {
	// The fast seeder delivers the whole 512 KiB in ~0.5 s. The slow seeder at
	// 16 KiB/s needs ~1 s per 16 KiB piece, so the up-to-4 requests queued
	// there take ~4 s unless endgame duplicates them to the fast peer.
	run := func(t *testing.T, endgame bool) (time.Duration, *coordinator.Status) {
		f := newFixture(t, 512<<10) // 32 pieces
		src := f.writeSource("src.bin", nil)
		slow := f.start("slow", peerOpts{seedFile: src, listener: impaired(t, faultnet.Impairment{BytesPerSec: 16 << 10})})
		fast := f.start("fast", peerOpts{seedFile: src, listener: impaired(t, faultnet.Impairment{BytesPerSec: 1 << 20})})
		d := f.start("d", peerOpts{
			peers: []string{slow.addr(), fast.addr()},
			ccfg: func(c *coordinator.Config) {
				c.Selector = scheduler.NewRandomPeer(3) // make sure the slow peer gets work
				c.RequestTimeout = 30 * time.Second     // so endgame, not timeouts, does the rescue
				c.Endgame.Enabled = endgame
			},
		})
		start := time.Now()
		f.waitComplete(30*time.Second, d)
		elapsed := time.Since(start)
		f.checkOutput(d)
		s := d.co.Status()
		t.Logf("endgame=%v: completed in %v; endgame requests %d, duplicate pieces %d, cancelled %d",
			endgame, elapsed.Round(time.Millisecond), s.Counters.EndgameRequests, s.Counters.DuplicatePieces, s.Counters.Cancelled)
		return elapsed, s
	}
	with, s := run(t, true)
	if s.Counters.EndgameRequests == 0 || !s.Endgame {
		t.Fatalf("expected endgame duplicate requests (got %d, endgame=%v)", s.Counters.EndgameRequests, s.Endgame)
	}
	without, s := run(t, false)
	if s.Counters.EndgameRequests != 0 {
		t.Fatal("endgame requests issued with endgame disabled")
	}
	if with >= without/2 {
		t.Fatalf("endgame %v vs no endgame %v; expected at least a 2x improvement", with, without)
	}
}

func TestUploadQueueCapRejectsBusyWithoutPenalty(t *testing.T) {
	f := newFixture(t, 512<<10)
	seed := f.start("seed", peerOpts{
		seedFile: f.writeSource("src.bin", nil),
		listener: impaired(t, faultnet.Impairment{BytesPerSec: 2 << 20}),
		ccfg:     func(c *coordinator.Config) { c.UploadQueueCap = 2 },
	})
	var ds []*testPeer
	for _, name := range []string{"d1", "d2", "d3", "d4"} {
		ds = append(ds, f.start(name, peerOpts{
			peers: []string{seed.addr()},
			ccfg:  func(c *coordinator.Config) { c.RequestTimeout = 30 * time.Second },
		}))
	}
	f.waitComplete(30*time.Second, ds...)
	for _, d := range ds {
		f.checkOutput(d)
	}
	waitFor(t, 5*time.Second, func() bool { return seed.co.Status().Counters.BusySent > 0 })
	busy := 0
	for _, d := range ds {
		s := d.co.Status()
		busy += s.Counters.BusyReceived
		for _, p := range s.Peers {
			if p.ID == seed.n.ID().Short() && (p.Reliability != 1 || p.Timeouts != 0) {
				t.Errorf("%s: seeder reliability %.2f, timeouts %d; busy rejects must not count as failures",
					d.out, p.Reliability, p.Timeouts)
			}
		}
	}
	if busy == 0 {
		t.Fatal("downloaders received no busy rejects with a queue cap of 2")
	}
	t.Logf("busy rejects: sent %d, received %d", seed.co.Status().Counters.BusySent, busy)
	for _, d := range ds {
		d.stop()
	}
	// No queue slots leak when requesters disconnect.
	waitFor(t, 5*time.Second, func() bool { return seed.co.Status().UploadQueue == 0 })
}

func TestLyingPeerIsBanned(t *testing.T) {
	f := newFixture(t, 512<<10)
	// Every piece in the liar's file is corrupted, but it claims to have them all.
	bad := bytes.Clone(f.data)
	for i := range f.m.NumPieces() {
		bad[f.m.PieceOffset(i)] ^= 0xFF
	}
	liarFile := filepath.Join(f.dir, "liar.bin")
	os.WriteFile(liarFile, bad, 0o644)
	liar := f.start("liar", peerOpts{seedFile: liarFile, claimAll: true})
	good := f.start("good", peerOpts{seedFile: f.writeSource("src.bin", nil)})
	d := f.start("d", peerOpts{
		peers: []string{liar.addr(), good.addr()},
		ccfg:  func(c *coordinator.Config) { c.Selector = scheduler.NewRandomPeer(5) },
	})
	f.waitComplete(30*time.Second, d)
	f.checkOutput(d)
	s := d.co.Status()
	if s.Counters.Banned != 1 || s.Rejected < 2 {
		t.Fatalf("banned=%d rejected=%d; want 1 ban after ≥2 invalid pieces", s.Counters.Banned, s.Rejected)
	}
	for _, p := range s.Peers {
		if p.ID == liar.n.ID().Short() {
			t.Fatal("banned peer still connected")
		}
	}
}

func TestCorruptedPieceIsRejected(t *testing.T) {
	f := newFixture(t, 512<<10)
	src := f.writeSource("src.bin", nil)
	// Flip one byte well past the handshake and BITFIELD, inside some PIECE payload.
	bad := f.start("bad", peerOpts{seedFile: src, listener: impaired(t, faultnet.Impairment{CorruptAt: 100 << 10})})
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

func TestPEXDiscoversIndirectPeers(t *testing.T) {
	f := newFixture(t, 256<<10)
	pex := func(c *coordinator.Config) { c.PEXInterval = 30 * time.Second }
	a := f.start("a", peerOpts{seedFile: f.writeSource("src.bin", nil), ccfg: pex})
	b := f.start("b", peerOpts{peers: []string{a.addr()}, ccfg: pex})
	waitFor(t, 5*time.Second, func() bool { return b.co.Connected(a.n.ID()) })
	// c only knows b; it must learn a's address from b's PEX.
	c := f.start("c", peerOpts{peers: []string{b.addr()}, ccfg: pex})
	waitFor(t, 10*time.Second, func() bool { return c.co.Connected(a.n.ID()) })
	f.waitComplete(20*time.Second, b, c)
	f.checkOutput(c)
}

func TestTrackerUnavailableAtStart(t *testing.T) {
	f := newFixture(t, 256<<10)
	ln, err := netx.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close() // nothing listens here yet
	url := "http://" + addr
	d := f.start("d", peerOpts{tracker: url})
	f.start("seed", peerOpts{seedFile: f.writeSource("src.bin", nil), tracker: url})
	time.Sleep(time.Second) // several failed announces with backoff
	if d.co.PeerCount() != 0 {
		t.Fatal("peers connected without a tracker")
	}
	ln, err = net.Listen("tcp", addr)
	if err != nil {
		t.Skipf("could not re-bind the reserved tracker port: %v", err)
	}
	srv := httptest.NewUnstartedServer(discovery.NewTracker(testLogger(t)).Handler())
	srv.Listener = ln
	srv.Start()
	defer srv.Close()
	f.waitComplete(20*time.Second, d)
	f.checkOutput(d)
}

// replicaPieces checks an acceptor's store and returns how many pieces it holds
// that verify against the manifest.
func (f *fixture) replicaPieces(p *testPeer) int {
	f.t.Helper()
	valid, err := p.st.Recheck(context.Background(), 2)
	if err != nil {
		f.t.Fatal(err)
	}
	if valid.Count() != p.co.Status().Have {
		f.t.Fatalf("acceptor reports %d pieces, %d verify on disk", p.co.Status().Have, valid.Count())
	}
	return valid.Count()
}

func TestReplicationReachesTargetAndRecoversFromLoss(t *testing.T) {
	f := newFixture(t, 512<<10) // 32 pieces
	seed := f.start("seed", peerOpts{seedFile: f.writeSource("src.bin", nil), replicas: 3})
	var acc []*testPeer
	for _, name := range []string{"r1", "r2", "r3"} {
		acc = append(acc, f.start(name, peerOpts{acceptor: true, peers: []string{seed.addr()}}))
	}
	n := f.m.NumPieces()
	reached := func() bool {
		r := seed.co.Status().Replication
		return r.UnderReplicated == 0 && r.MinVerified >= 2
	}
	start := time.Now()
	waitFor(t, 20*time.Second, reached)
	t.Logf("2 verified extra copies of %d pieces after %v: %+v", n, time.Since(start).Round(time.Millisecond),
		seed.co.Status().Replication.Counters)

	total := 0
	for _, a := range acc {
		total += f.replicaPieces(a)
		if a.co.Status().Complete {
			t.Errorf("acceptor holds the whole file; replicas should be spread")
		}
	}
	if total != 2*n {
		t.Fatalf("acceptors hold %d piece copies; want exactly %d (target 3 incl. seed)", total, 2*n)
	}

	// Lose one replica holder: its copies stop counting and are re-created on
	// the others. On loopback this finishes faster than the status snapshot
	// interval, so it is observed through the cumulative counters.
	lost := f.replicaPieces(acc[0])
	before := seed.co.Status().Replication.Counters
	start = time.Now()
	acc[0].stop()
	waitFor(t, 20*time.Second, func() bool {
		r := seed.co.Status().Replication
		return reached() && r.Counters.ProofsOK >= before.ProofsOK+lost
	})
	after := seed.co.Status().Replication.Counters
	t.Logf("re-replicated %d lost copies within %v (offers %d, proofs %d)", lost,
		time.Since(start).Round(time.Millisecond), after.OffersSent-before.OffersSent, after.ProofsOK-before.ProofsOK)
	if after.OffersSent-before.OffersSent != lost {
		t.Errorf("sent %d new offers for %d lost copies", after.OffersSent-before.OffersSent, lost)
	}
	for _, a := range acc[1:] {
		if got := f.replicaPieces(a); got != n {
			t.Errorf("remaining acceptor holds %d/%d pieces", got, n)
		}
	}
}

func TestLyingReplicaHolderFailsPossessionProof(t *testing.T) {
	f := newFixture(t, 256<<10) // 16 pieces
	bad := bytes.Clone(f.data)
	for i := range f.m.NumPieces() {
		bad[f.m.PieceOffset(i)] ^= 0xFF
	}
	liarFile := filepath.Join(f.dir, "liar.bin")
	os.WriteFile(liarFile, bad, 0o644)
	seed := f.start("seed", peerOpts{seedFile: f.writeSource("src.bin", nil), replicas: 2})
	liar := f.start("liar", peerOpts{seedFile: liarFile, claimAll: true, peers: []string{seed.addr()}})
	honest := f.start("honest", peerOpts{acceptor: true, peers: []string{seed.addr()}})
	waitFor(t, 20*time.Second, func() bool {
		r := seed.co.Status().Replication
		return r.UnderReplicated == 0 && r.MinVerified >= 1
	})
	s := seed.co.Status()
	if s.Replication.Counters.ProofsFailed < 2 || s.Counters.Banned != 1 {
		t.Fatalf("proofs failed %d, banned %d; the liar's claims must fail and get it banned",
			s.Replication.Counters.ProofsFailed, s.Counters.Banned)
	}
	if seed.co.Connected(liar.n.ID()) {
		t.Fatal("liar still connected")
	}
	if got := f.replicaPieces(honest); got != f.m.NumPieces() {
		t.Fatalf("honest acceptor holds %d/%d pieces; it should carry every copy", got, f.m.NumPieces())
	}
}

func TestAllCopiesLostThenSourceReturns(t *testing.T) {
	f := newFixture(t, 1<<20)
	src := f.writeSource("src.bin", nil)
	s1 := f.start("s1", peerOpts{seedFile: src, listener: impaired(t, faultnet.Impairment{BytesPerSec: 256 << 10})})
	d := f.start("d", peerOpts{peers: []string{s1.addr()}})
	waitFor(t, 10*time.Second, func() bool { return d.co.Status().Have >= d.co.Status().Pieces/4 })
	s1.stop()
	waitFor(t, 5*time.Second, func() bool { return d.co.Status().Unavailable > 0 })
	time.Sleep(time.Second)
	if s := d.co.Status(); s.Complete || s.Unavailable == 0 {
		t.Fatalf("with no source left: complete=%v unavailable=%d", s.Complete, s.Unavailable)
	}
	t.Logf("no source for %d pieces; download waits instead of failing", d.co.Status().Unavailable)
	// A new seeder appears and connects to the waiting downloader.
	f.start("s2", peerOpts{seedFile: src, peers: []string{d.addr()}})
	f.waitComplete(20*time.Second, d)
	f.checkOutput(d)
	if u := d.co.Status().Unavailable; u != 0 {
		t.Fatalf("unavailable=%d after completion", u)
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
