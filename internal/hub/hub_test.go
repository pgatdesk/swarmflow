package hub

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pgatdesk/swarmflow/internal/api"
	"github.com/pgatdesk/swarmflow/internal/bitfield"
	"github.com/pgatdesk/swarmflow/internal/coordinator"
	"github.com/pgatdesk/swarmflow/internal/dataset"
	"github.com/pgatdesk/swarmflow/internal/discovery"
	"github.com/pgatdesk/swarmflow/internal/events"
	"github.com/pgatdesk/swarmflow/internal/faultnet"
	"github.com/pgatdesk/swarmflow/internal/manifest"
	"github.com/pgatdesk/swarmflow/internal/metrics"
	"github.com/pgatdesk/swarmflow/internal/netx"
	"github.com/pgatdesk/swarmflow/internal/node"
	"github.com/pgatdesk/swarmflow/internal/store"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

type monitoredPeer struct {
	n       *node.Node
	co      *coordinator.Coordinator
	apiAddr string
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	st      *store.Store
}

// startPeer wires a peer the way the CLI does: node, coordinator with events
// and metrics, and the monitoring API announced to the tracker.
func startPeer(t *testing.T, m *manifest.Manifest, st *store.Store, have bitfield.Bitfield, tracker string, ln *faultnet.Listener) *monitoredPeer {
	t.Helper()
	apiLn, err := netx.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ncfg := node.DefaultConfig()
	ncfg.TrackerURL, ncfg.AnnounceInterval, ncfg.LonelyInterval = tracker, time.Second, 200*time.Millisecond
	if ln != nil {
		ncfg.Listener = ln
	}
	ncfg.APIPort = netip.MustParseAddrPort(apiLn.Addr().String()).Port()
	n, err := node.New(ncfg, quiet)
	if err != nil {
		t.Fatal(err)
	}
	var ref atomic.Pointer[coordinator.Coordinator]
	ring := events.NewRing(1000)
	reg, obs := metrics.NewRegistry(n.ID().Short(), m.ID().Short(), ref.Load)
	ccfg := coordinator.DefaultConfig()
	ccfg.Events, ccfg.Observer, ccfg.PEXInterval = ring, obs, 0
	co := coordinator.New(m, st, have, n.ID(), ccfg, quiet)
	ref.Store(co)
	n.AddSwarm(co)
	ctx, cancel := context.WithCancel(context.Background())
	p := &monitoredPeer{n: n, co: co, apiAddr: apiLn.Addr().String(), cancel: cancel, st: st}
	srv := &api.Server{Peer: n.ID().Short(), Role: "test", Coord: ref.Load, Events: ring, Reg: reg, Log: quiet}
	p.wg.Go(func() { co.Run(ctx) })
	p.wg.Go(func() { n.Run(ctx) })
	p.wg.Go(func() { srv.Serve(ctx, apiLn) })
	t.Cleanup(p.stop)
	return p
}

func (p *monitoredPeer) stop() {
	p.cancel()
	p.wg.Wait()
	p.st.Close()
}

func get[T any](t *testing.T, url string) T {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var v T
	if err := json.NewDecoder(resp.Body).Decode(&v); err != nil {
		t.Fatalf("%s: %v", url, err)
	}
	return v
}

func waitFor(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	for end := time.Now().Add(d); time.Now().Before(end); time.Sleep(50 * time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestHubObservesRealPeers(t *testing.T) {
	dir := t.TempDir()
	data := dataset.Bytes(4<<20, 3)
	m, _ := manifest.Build(bytes.NewReader(data), "p.bin", int64(len(data)), 64<<10)
	src := filepath.Join(dir, "src.bin")
	os.WriteFile(src, data, 0o644)

	tr := httptest.NewServer(discovery.NewTracker(quiet).Handler())
	defer tr.Close()

	seedStore, _ := store.OpenComplete(src, m)
	all, _ := seedStore.Recheck(context.Background(), 2)
	slow, _ := faultnet.Listen("127.0.0.1:0", faultnet.Impairment{BytesPerSec: 2 << 20}) // ~2 s transfer
	seed := startPeer(t, m, seedStore, all, tr.URL, slow)
	out := filepath.Join(dir, "d", "p.bin")
	os.MkdirAll(filepath.Dir(out), 0o755)
	dlStore, _, _ := store.Create(out+".part", out, m)
	dl := startPeer(t, m, dlStore, bitfield.New(m.NumPieces()), tr.URL, nil)

	h := New(tr.URL, 200*time.Millisecond, quiet)
	hs := httptest.NewServer(h.Handler())
	defer hs.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.Run(ctx)

	// Mid-transfer the edge must show data flowing seed → downloader.
	seedID, dlID := seed.n.ID().Short(), dl.n.ID().Short()
	waitFor(t, 10*time.Second, "a live edge with traffic", func() bool {
		edges := get[[]Edge](t, hs.URL+"/api/topology/edges")
		return len(edges) == 1 && strings.Contains(edges[0].Secondary, "both sides") && edges[0].MainStat != "0.00 MiB/s"
	})
	select {
	case <-dl.co.Complete():
	case <-time.After(20 * time.Second):
		t.Fatal("download did not complete")
	}

	if sd := get[[]sdTarget](t, hs.URL+"/sd"); len(sd) != 2 {
		t.Fatalf("service discovery: %+v", sd)
	}
	waitFor(t, 5*time.Second, "downloader at 100%", func() bool {
		for _, r := range get[[]PeerRow](t, hs.URL+"/api/peers") {
			if r.ID == dlID && r.ProgressPct == 100 && r.State == "online" {
				return true
			}
		}
		return false
	})
	evs := get[[]Event](t, hs.URL+"/api/events?limit=1000&type=piece_scheduled")
	if len(evs) == 0 || !strings.Contains(evs[0].Message, "rarest-first") || evs[0].Peer != dlID {
		t.Fatalf("scheduling events: %d, first %+v", len(evs), evs)
	}
	waitFor(t, 5*time.Second, "download_complete event", func() bool {
		return len(get[[]Event](t, hs.URL+"/api/events?type=download_complete")) == 1
	})

	resp, err := http.Get("http://" + dl.apiAddr + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	for _, want := range []string{
		`swarmflow_pieces_have{peer="` + dlID + `"`,
		`swarmflow_request_duration_seconds_count{outcome="ok",peer="` + dlID + `"`,
		`swarmflow_source_bytes_total{peer="` + dlID + `",remote="` + seedID + `"`,
	} {
		if !strings.Contains(string(body), want) {
			t.Errorf("metrics missing %s", want)
		}
	}

	// The seeder dies: the hub marks it unreachable and stops drawing its connection.
	seed.stop()
	waitFor(t, 10*time.Second, "seeder unreachable, edge gone", func() bool {
		var state string
		for _, r := range get[[]PeerRow](t, hs.URL+"/api/peers") {
			if r.ID == seedID {
				state = r.State
			}
		}
		return state == "unreachable" && len(get[[]Edge](t, hs.URL+"/api/topology/edges")) == 0
	})
	if len(get[[]Event](t, hs.URL+"/api/events?type=peer_disconnected")) == 0 {
		t.Fatal("no disconnect event from the downloader")
	}
}
