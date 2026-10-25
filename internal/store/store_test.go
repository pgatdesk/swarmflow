package store

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/pgatdesk/swarmflow/internal/dataset"
	"github.com/pgatdesk/swarmflow/internal/manifest"
)

func TestWriteRecheckFinalize(t *testing.T) {
	data := dataset.Bytes(5*4096+10, 3)
	m, err := manifest.Build(bytes.NewReader(data), "f.bin", int64(len(data)), 4096)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	part, final := filepath.Join(dir, "f.bin.part"), filepath.Join(dir, "f.bin")
	st, existed, err := Create(part, final, m)
	if err != nil || existed {
		t.Fatalf("create: %v existed=%v", err, existed)
	}
	piece := func(i int) []byte { o := m.PieceOffset(i); return data[o : o+int64(m.PieceLen(i))] }
	for _, i := range []int{0, 2, 5} {
		if err := st.WritePiece(i, piece(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.WritePiece(1, piece(1)[:10]); err == nil {
		t.Fatal("short write accepted")
	}
	st.Close()

	// Reopening finds exactly the written pieces.
	st, existed, err = Create(part, final, m)
	if err != nil || !existed {
		t.Fatalf("reopen: %v existed=%v", err, existed)
	}
	have, err := st.Recheck(context.Background(), 3)
	if err != nil {
		t.Fatal(err)
	}
	if have.Count() != 3 || !have.Has(0) || !have.Has(2) || !have.Has(5) {
		t.Fatalf("recheck found %08b", have.Bytes())
	}
	for _, i := range []int{1, 3, 4} {
		if err := st.WritePiece(i, piece(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.Finalize(); err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if _, err := os.Stat(part); !os.IsNotExist(err) {
		t.Fatal(".part still exists after finalize")
	}
	got, _ := os.ReadFile(final)
	if !bytes.Equal(got, data) {
		t.Fatal("final file differs from source")
	}
	if err := st.WritePiece(0, piece(0)); err != ErrReadOnly {
		t.Fatalf("write after finalize: %v", err)
	}
	if p, err := st.ReadPiece(5); err != nil || !bytes.Equal(p, piece(5)) {
		t.Fatalf("read after finalize: %v", err)
	}
}

func TestOpenCompleteSizeMismatch(t *testing.T) {
	data := dataset.Bytes(8192, 1)
	m, _ := manifest.Build(bytes.NewReader(data), "f.bin", 8192, 4096)
	path := filepath.Join(t.TempDir(), "f.bin")
	os.WriteFile(path, data[:5000], 0o644)
	if _, err := OpenComplete(path, m); err == nil {
		t.Fatal("size mismatch accepted")
	}
}
