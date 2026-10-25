package store

import (
	"bytes"
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"

	"github.com/pgatdesk/swarmflow/internal/dataset"
	"github.com/pgatdesk/swarmflow/internal/manifest"
)

const benchPiece = 256 << 10

func benchStore(b *testing.B) (*Store, []byte) {
	b.Helper()
	data := dataset.Bytes(64*benchPiece, 1)
	m, err := manifest.Build(bytes.NewReader(data), "f.bin", int64(len(data)), benchPiece)
	if err != nil {
		b.Fatal(err)
	}
	dir := b.TempDir()
	if d := os.Getenv("SWARMFLOW_BENCH_DIR"); d != "" { // measure a specific disk
		dir, _ = os.MkdirTemp(d, "bench")
		b.Cleanup(func() { os.RemoveAll(dir) })
	}
	st, _, err := Create(filepath.Join(dir, "f.part"), filepath.Join(dir, "f.bin"), m)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { st.Close() })
	return st, data
}

// BenchmarkWritePieceSynced is what the download path does per piece: positioned write + flush.
func BenchmarkWritePieceSynced(b *testing.B) {
	st, data := benchStore(b)
	b.SetBytes(benchPiece)
	for i := 0; b.Loop(); i++ {
		p := i % 64
		if err := st.WritePiece(p, data[p*benchPiece:(p+1)*benchPiece]); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkWritePieceNoSync isolates the cost of the flush.
func BenchmarkWritePieceNoSync(b *testing.B) {
	st, data := benchStore(b)
	b.SetBytes(benchPiece)
	for i := 0; b.Loop(); i++ {
		p := i % 64
		if _, err := st.f.WriteAt(data[p*benchPiece:(p+1)*benchPiece], st.m.PieceOffset(p)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSHA256Piece(b *testing.B) {
	data := dataset.Bytes(benchPiece, 2)
	b.SetBytes(benchPiece)
	for b.Loop() {
		sha256.Sum256(data)
	}
}

// BenchmarkWritePieceSyncedParallel models the verify pool: several workers
// flushing different pieces of the same file concurrently.
func BenchmarkWritePieceSyncedParallel(b *testing.B) {
	st, data := benchStore(b)
	b.SetBytes(benchPiece)
	b.SetParallelism(1) // GOMAXPROCS goroutines; run with -cpu 2,4 to vary
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			p := i % 64
			i++
			if err := st.WritePiece(p, data[p*benchPiece:(p+1)*benchPiece]); err != nil {
				b.Error(err)
				return
			}
		}
	})
}
