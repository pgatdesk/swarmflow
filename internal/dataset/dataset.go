// Package dataset generates reproducible synthetic test files: the same size
// and seed always produce the same bytes, on any machine.
package dataset

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"strconv"
	"strings"
)

// Write writes size pseudo-random bytes derived from seed.
func Write(w io.Writer, size int64, seed uint64) error {
	var key [32]byte
	binary.LittleEndian.PutUint64(key[:], seed)
	_, err := io.CopyN(w, rand.NewChaCha8(key), size)
	return err
}

// Bytes returns the dataset in memory, for tests.
func Bytes(size int, seed uint64) []byte {
	var sb strings.Builder
	sb.Grow(size)
	if err := Write(&sb, int64(size), seed); err != nil {
		panic(err)
	}
	return []byte(sb.String())
}

func WriteFile(path string, size int64, seed uint64) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	bw := bufio.NewWriterSize(f, 1<<20)
	if err := Write(bw, size, seed); err != nil {
		f.Close()
		return err
	}
	if err := bw.Flush(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// ParseSize accepts plain byte counts and KiB/MiB/GiB suffixes (also KB/MB/GB,
// treated as binary units).
func ParseSize(s string) (int64, error) {
	s = strings.TrimSpace(s)
	mult := int64(1)
	for _, u := range []struct {
		suffix string
		mult   int64
	}{{"KiB", 1 << 10}, {"MiB", 1 << 20}, {"GiB", 1 << 30}, {"KB", 1 << 10}, {"MB", 1 << 20}, {"GB", 1 << 30}, {"K", 1 << 10}, {"M", 1 << 20}, {"G", 1 << 30}} {
		if strings.HasSuffix(s, u.suffix) {
			s, mult = strings.TrimSuffix(s, u.suffix), u.mult
			break
		}
	}
	n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("invalid size %q", s)
	}
	return n * mult, nil
}
