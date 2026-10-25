// Package manifest defines the trusted metadata of a swarm: file name, size,
// piece size and the SHA-256 of every piece. The swarm ID is the SHA-256 of the
// manifest's canonical JSON encoding, so a peer that knows the swarm ID can
// verify a manifest obtained from anyone, and every piece against it.
package manifest

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/pgatdesk/swarmflow/internal/protocol"
)

const (
	FormatVersion    = 1
	DefaultPieceSize = 256 << 10
	MinPieceSize     = 4 << 10
	maxManifestBytes = 64 << 20
	maxPieces        = 1 << 31
)

type Manifest struct {
	name      string
	size      int64
	pieceSize int
	hashes    [][32]byte
	id        protocol.SwarmID
}

// wire is the JSON form. Fields are declared in alphabetical order of their JSON
// names, so encoding/json's compact output is the canonical encoding.
type wire struct {
	FormatVersion int      `json:"format_version"`
	Name          string   `json:"name"`
	PieceHashes   []string `json:"piece_hashes"`
	PieceSize     int      `json:"piece_size"`
	Size          int64    `json:"size"`
}

// New validates the fields and computes the swarm ID.
func New(name string, size int64, pieceSize int, hashes [][32]byte) (*Manifest, error) {
	if err := validateName(name); err != nil {
		return nil, err
	}
	if pieceSize < MinPieceSize || pieceSize > protocol.MaxPieceSize || pieceSize&(pieceSize-1) != 0 {
		return nil, fmt.Errorf("manifest: piece size %d must be a power of two in [%d, %d]",
			pieceSize, MinPieceSize, protocol.MaxPieceSize)
	}
	if size <= 0 {
		return nil, fmt.Errorf("manifest: size must be positive, got %d", size)
	}
	n := (size + int64(pieceSize) - 1) / int64(pieceSize)
	if n > maxPieces {
		return nil, fmt.Errorf("manifest: %d pieces exceeds limit", n)
	}
	if int64(len(hashes)) != n {
		return nil, fmt.Errorf("manifest: %d piece hashes for %d pieces", len(hashes), n)
	}
	m := &Manifest{name: name, size: size, pieceSize: pieceSize, hashes: hashes}
	m.id = sha256.Sum256(m.Canonical())
	return m, nil
}

func validateName(name string) error {
	switch {
	case name == "" || name == "." || name == "..":
		return fmt.Errorf("manifest: invalid file name %q", name)
	case len(name) > 255:
		return fmt.Errorf("manifest: file name longer than 255 bytes")
	case strings.ContainsAny(name, `/\:*?"<>|`):
		return fmt.Errorf("manifest: file name %q contains a path separator or reserved character", name)
	}
	for _, r := range name {
		if r < 0x20 || r == 0x7f {
			return fmt.Errorf("manifest: file name contains a control character")
		}
	}
	return nil
}

// Build hashes size bytes from r in pieceSize chunks.
func Build(r io.Reader, name string, size int64, pieceSize int) (*Manifest, error) {
	if pieceSize <= 0 || size <= 0 {
		return New(name, size, pieceSize, nil) // reports the validation error
	}
	n := (size + int64(pieceSize) - 1) / int64(pieceSize)
	hashes := make([][32]byte, 0, n)
	buf := make([]byte, pieceSize)
	for remaining := size; remaining > 0; {
		chunk := buf[:min(int64(pieceSize), remaining)]
		if _, err := io.ReadFull(r, chunk); err != nil {
			return nil, fmt.Errorf("manifest: reading piece %d: %w", len(hashes), err)
		}
		hashes = append(hashes, sha256.Sum256(chunk))
		remaining -= int64(len(chunk))
	}
	return New(name, size, pieceSize, hashes)
}

// BuildFile builds a manifest for the file at path, named after its base name.
func BuildFile(path string, pieceSize int) (*Manifest, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	return Build(f, fi.Name(), fi.Size(), pieceSize)
}

func Parse(b []byte) (*Manifest, error) {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	var w wire
	if err := dec.Decode(&w); err != nil {
		return nil, fmt.Errorf("manifest: %w", err)
	}
	if w.FormatVersion != FormatVersion {
		return nil, fmt.Errorf("manifest: unsupported format version %d", w.FormatVersion)
	}
	hashes := make([][32]byte, len(w.PieceHashes))
	for i, h := range w.PieceHashes {
		if len(h) != 64 {
			return nil, fmt.Errorf("manifest: piece hash %d has %d hex digits", i, len(h))
		}
		if _, err := hex.Decode(hashes[i][:], []byte(h)); err != nil {
			return nil, fmt.Errorf("manifest: piece hash %d: %w", i, err)
		}
	}
	return New(w.Name, w.Size, w.PieceSize, hashes)
}

func Load(path string) (*Manifest, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, maxManifestBytes+1))
	if err != nil {
		return nil, err
	}
	if len(b) > maxManifestBytes {
		return nil, errors.New("manifest: file too large")
	}
	return Parse(b)
}

func (m *Manifest) wire() wire {
	hs := make([]string, len(m.hashes))
	for i, h := range m.hashes {
		hs[i] = hex.EncodeToString(h[:])
	}
	return wire{FormatVersion: FormatVersion, Name: m.name, PieceHashes: hs, PieceSize: m.pieceSize, Size: m.size}
}

// Canonical returns the compact, key-sorted JSON encoding the swarm ID is computed over.
func (m *Manifest) Canonical() []byte {
	b, err := json.Marshal(m.wire())
	if err != nil {
		panic(err) // the wire struct contains only strings and integers
	}
	return b
}

// Save writes an indented, human-readable manifest; the swarm ID does not depend on formatting.
func (m *Manifest) Save(path string) error {
	b, err := json.MarshalIndent(m.wire(), "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}

func (m *Manifest) ID() protocol.SwarmID { return m.id }
func (m *Manifest) Name() string         { return m.name }
func (m *Manifest) Size() int64          { return m.size }
func (m *Manifest) PieceSize() int       { return m.pieceSize }
func (m *Manifest) NumPieces() int       { return len(m.hashes) }
func (m *Manifest) Hash(i int) [32]byte  { return m.hashes[i] }

func (m *Manifest) PieceOffset(i int) int64 { return int64(i) * int64(m.pieceSize) }

func (m *Manifest) PieceLen(i int) int {
	if i == len(m.hashes)-1 {
		return int(m.size - m.PieceOffset(i))
	}
	return m.pieceSize
}

// Verify reports whether data is exactly piece i.
func (m *Manifest) Verify(i int, data []byte) bool {
	return i >= 0 && i < len(m.hashes) && len(data) == m.PieceLen(i) && sha256.Sum256(data) == m.hashes[i]
}
