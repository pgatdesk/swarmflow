package discovery

import (
	"context"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/pgatdesk/swarmflow/internal/protocol"
)

func TestAnnounceLifecycle(t *testing.T) {
	tr := NewTracker(slog.New(slog.NewTextHandler(io.Discard, nil)))
	now := time.Unix(1000, 0)
	tr.now = func() time.Time { return now }
	srv := httptest.NewServer(tr.Handler())
	defer srv.Close()
	c := NewClient(srv.URL)
	ctx := context.Background()

	swarm := protocol.SwarmID{1}.String()
	a, b := protocol.PeerID{0xA}.String(), protocol.PeerID{0xB}.String()

	resp, err := c.Announce(ctx, AnnounceRequest{SwarmID: swarm, PeerID: a, Port: 7001, Event: EventStarted})
	if err != nil || len(resp.Peers) != 0 || resp.IntervalSec != 30 {
		t.Fatalf("first announce: %+v %v", resp, err)
	}
	resp, err = c.Announce(ctx, AnnounceRequest{SwarmID: swarm, PeerID: b, Port: 7002, IP: "10.0.0.2", APIPort: 8002})
	if err != nil || len(resp.Peers) != 1 || resp.Peers[0].PeerID != a || resp.Peers[0].Addr != "127.0.0.1:7001" {
		t.Fatalf("second announce: %+v %v", resp, err)
	}
	resp, _ = c.Announce(ctx, AnnounceRequest{SwarmID: swarm, PeerID: a, Port: 7001})
	if len(resp.Peers) != 1 || resp.Peers[0].Addr != "10.0.0.2:7002" || resp.Peers[0].APIAddr != "10.0.0.2:8002" {
		t.Fatalf("advertised address not used: %+v", resp)
	}

	// b stops; a no longer sees it.
	c.Announce(ctx, AnnounceRequest{SwarmID: swarm, PeerID: b, Port: 7002, Event: EventStopped})
	if resp, _ = c.Announce(ctx, AnnounceRequest{SwarmID: swarm, PeerID: a, Port: 7001}); len(resp.Peers) != 0 {
		t.Fatalf("stopped peer still listed: %+v", resp)
	}

	// b re-announces, then goes silent past the TTL.
	c.Announce(ctx, AnnounceRequest{SwarmID: swarm, PeerID: b, Port: 7002})
	now = now.Add(tr.TTL + time.Second)
	if resp, _ = c.Announce(ctx, AnnounceRequest{SwarmID: swarm, PeerID: a, Port: 7001}); len(resp.Peers) != 0 {
		t.Fatalf("expired peer still listed: %+v", resp)
	}
}

func TestAnnounceValidation(t *testing.T) {
	tr := NewTracker(slog.New(slog.NewTextHandler(io.Discard, nil)))
	srv := httptest.NewServer(tr.Handler())
	defer srv.Close()
	c := NewClient(srv.URL)
	good := AnnounceRequest{SwarmID: protocol.SwarmID{1}.String(), PeerID: protocol.PeerID{2}.String(), Port: 1}
	bad := []AnnounceRequest{
		{SwarmID: "zz", PeerID: good.PeerID, Port: 1},
		{SwarmID: good.SwarmID, PeerID: "short", Port: 1},
		{SwarmID: good.SwarmID, PeerID: good.PeerID, Port: 0},
		{SwarmID: good.SwarmID, PeerID: good.PeerID, Port: 1, IP: "not-an-ip"},
	}
	for _, r := range bad {
		if _, err := c.Announce(context.Background(), r); err == nil || !strings.Contains(err.Error(), "400") {
			t.Errorf("%+v: got %v, want 400", r, err)
		}
	}
	if _, err := c.Announce(context.Background(), good); err != nil {
		t.Fatal(err)
	}
}
