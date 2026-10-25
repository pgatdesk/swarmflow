package statedb

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/pgatdesk/swarmflow/internal/protocol"
)

// BenchmarkMarkPersisted measures one durable piece record (a WAL commit with
// synchronous=FULL), the per-piece cost the download path pays after the data flush.
func BenchmarkMarkPersisted(b *testing.B) {
	dir := b.TempDir()
	if d := os.Getenv("SWARMFLOW_BENCH_DIR"); d != "" {
		dir, _ = os.MkdirTemp(d, "bench")
		b.Cleanup(func() { os.RemoveAll(dir) })
	}
	d, err := Open(filepath.Join(dir, "state.db"))
	if err != nil {
		b.Fatal(err)
	}
	defer d.Close()
	id := protocol.SwarmID{1}
	d.UpsertSwarm(Swarm{ID: id, Name: "x", Size: 1, PieceSize: 4096, Pieces: 1 << 20, Manifest: []byte("{}"), Status: StatusDownloading})
	i := 0
	for b.Loop() {
		if err := d.MarkPersisted(id, i, [32]byte{}); err != nil {
			b.Fatal(err)
		}
		i++
	}
}
