package manifest

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pgatdesk/swarmflow/internal/dataset"
)

func build(t *testing.T, size, pieceSize int) (*Manifest, []byte) {
	t.Helper()
	data := dataset.Bytes(size, 7)
	m, err := Build(bytes.NewReader(data), "data.bin", int64(size), pieceSize)
	if err != nil {
		t.Fatal(err)
	}
	return m, data
}

func TestPieceGeometry(t *testing.T) {
	m, data := build(t, 10*4096+100, 4096)
	if m.NumPieces() != 11 || m.PieceLen(10) != 100 || m.PieceLen(0) != 4096 || m.PieceOffset(10) != 40960 {
		t.Fatalf("geometry: n=%d last=%d", m.NumPieces(), m.PieceLen(10))
	}
	for i := range m.NumPieces() {
		off := m.PieceOffset(i)
		if !m.Verify(i, data[off:off+int64(m.PieceLen(i))]) {
			t.Fatalf("piece %d does not verify", i)
		}
	}
	bad := bytes.Clone(data[:4096])
	bad[100] ^= 1
	if m.Verify(0, bad) {
		t.Fatal("corrupted piece verified")
	}
	if m.Verify(0, data[:4095]) || m.Verify(11, nil) || m.Verify(-1, nil) {
		t.Fatal("wrong length or index verified")
	}
}

func TestSaveLoadAndIDIndependentOfFormatting(t *testing.T) {
	m, _ := build(t, 50000, 4096)
	path := filepath.Join(t.TempDir(), "x.swarm")
	if err := m.Save(path); err != nil {
		t.Fatal(err)
	}
	got, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID() != m.ID() {
		t.Fatal("ID changed after save/load")
	}
	compact, err := Parse(m.Canonical())
	if err != nil || compact.ID() != m.ID() {
		t.Fatalf("canonical parse: %v", err)
	}
}

func TestCanonicalIsSortedCompactJSON(t *testing.T) {
	m, _ := build(t, 5000, 4096)
	c := string(m.Canonical())
	if strings.ContainsAny(c, " \n") {
		t.Fatal("canonical encoding has whitespace")
	}
	keys := []string{`"format_version"`, `"name"`, `"piece_hashes"`, `"piece_size"`, `"size"`}
	last := -1
	for _, k := range keys {
		i := strings.Index(c, k)
		if i <= last {
			t.Fatalf("key %s out of order in %s", k, c)
		}
		last = i
	}
}

func TestIDChangesWithContent(t *testing.T) {
	a, _ := build(t, 5000, 4096)
	data := dataset.Bytes(5000, 8)
	b, _ := Build(bytes.NewReader(data), "data.bin", 5000, 4096)
	if a.ID() == b.ID() {
		t.Fatal("different content, same swarm ID")
	}
}

func TestParseRejects(t *testing.T) {
	m, _ := build(t, 5000, 4096)
	var w map[string]any
	json.Unmarshal(m.Canonical(), &w)
	mutate := func(k string, v any) []byte {
		c := map[string]any{}
		for kk, vv := range w {
			c[kk] = vv
		}
		c[k] = v
		b, _ := json.Marshal(c)
		return b
	}
	cases := map[string][]byte{
		"path traversal name": mutate("name", "../evil"),
		"windows separator":   mutate("name", `a\b`),
		"bad piece size":      mutate("piece_size", 5000),
		"size mismatch":       mutate("size", 999999),
		"bad version":         mutate("format_version", 2),
		"unknown field":       mutate("extra", 1),
		"short hash":          mutate("piece_hashes", []string{"abcd", "ef"}),
	}
	for name, b := range cases {
		if _, err := Parse(b); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
