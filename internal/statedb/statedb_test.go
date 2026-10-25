package statedb

import (
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/pgatdesk/swarmflow/internal/protocol"
)

func open(t *testing.T, path string) *DB {
	t.Helper()
	d, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Close() })
	return d
}

func TestSwarmAndPiecesSurviveReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	d := open(t, path)
	id := protocol.SwarmID{7}
	s := Swarm{ID: id, Name: "f.bin", Size: 1000, PieceSize: 4096, Pieces: 1, Manifest: []byte("{}"),
		PartPath: "f.bin.part", FinalPath: "f.bin", Status: StatusDownloading}
	if err := d.UpsertSwarm(s); err != nil {
		t.Fatal(err)
	}
	for _, i := range []int{3, 1, 2} {
		if err := d.MarkPersisted(id, i, [32]byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := d.MarkPersisted(id, 2, [32]byte{2}); err != nil { // idempotent
		t.Fatal(err)
	}
	if err := d.Unmark(id, []int{3}); err != nil {
		t.Fatal(err)
	}
	d.Close()

	d = open(t, path)
	got, ok, err := d.Swarm(id)
	if err != nil || !ok || got.Name != "f.bin" || got.Status != StatusDownloading || got.Created.IsZero() {
		t.Fatalf("swarm after reopen: %+v ok=%v err=%v", got, ok, err)
	}
	ps, err := d.Persisted(id)
	if err != nil || len(ps) != 2 || ps[0].Index != 1 || ps[1].Index != 2 || ps[1].Hash != [32]byte{2} {
		t.Fatalf("pieces after reopen: %+v %v", ps, err)
	}
	if err := d.SetStatus(id, StatusComplete); err != nil {
		t.Fatal(err)
	}
	got, _, _ = d.Swarm(id)
	if got.Status != StatusComplete || got.Completed.IsZero() {
		t.Fatalf("status not updated: %+v", got)
	}
	if _, ok, _ := d.Swarm(protocol.SwarmID{8}); ok {
		t.Fatal("unknown swarm found")
	}
}

func TestPiecesRequireSwarm(t *testing.T) {
	d := open(t, filepath.Join(t.TempDir(), "state.db"))
	if err := d.MarkPersisted(protocol.SwarmID{1}, 0, [32]byte{}); err == nil {
		t.Fatal("piece recorded for a swarm with no record (foreign key not enforced)")
	}
}

func TestConcurrentMarks(t *testing.T) {
	d := open(t, filepath.Join(t.TempDir(), "state.db"))
	id := protocol.SwarmID{1}
	d.UpsertSwarm(Swarm{ID: id, Name: "x", Size: 1, PieceSize: 4096, Pieces: 200, Manifest: []byte("{}"), Status: StatusDownloading})
	var wg sync.WaitGroup
	for w := range 8 {
		wg.Go(func() {
			for i := w; i < 200; i += 8 {
				if err := d.MarkPersisted(id, i, [32]byte{}); err != nil {
					t.Error(err)
				}
			}
		})
	}
	wg.Wait()
	if n, err := d.CountPersisted(id); err != nil || n != 200 {
		t.Fatalf("count %d err %v", n, err)
	}
}

func TestRecentPeers(t *testing.T) {
	d := open(t, filepath.Join(t.TempDir(), "state.db"))
	id := protocol.SwarmID{1}
	now := time.Now()
	d.RecordPeer(id, "10.0.0.1:1", protocol.PeerID{1}, now.Add(-time.Hour))
	d.RecordPeer(id, "10.0.0.2:2", protocol.PeerID{2}, now)
	d.RecordPeer(id, "10.0.0.3:3", protocol.PeerID{3}, now.Add(-time.Minute))
	got, err := d.RecentPeers(id, now.Add(-10*time.Minute), 10)
	if err != nil || len(got) != 2 || got[0] != "10.0.0.2:2" || got[1] != "10.0.0.3:3" {
		t.Fatalf("recent peers %v %v", got, err)
	}
}
