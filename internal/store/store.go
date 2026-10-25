// Package store keeps a swarm's file data on disk. Downloads go to a
// preallocated "<name>.part" file written piece by piece at fixed offsets; on
// completion it is renamed to its final name and reopened read-only for seeding.
package store

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/pgatdesk/swarmflow/internal/bitfield"
	"github.com/pgatdesk/swarmflow/internal/manifest"
)

var ErrReadOnly = errors.New("store: read-only")

type Store struct {
	// mu is held for reading by piece I/O (positioned reads and writes to
	// distinct offsets are safe concurrently) and for writing by Finalize/Close.
	mu        sync.RWMutex
	f         *os.File
	m         *manifest.Manifest
	path      string
	finalPath string
	writable  bool
}

// Create opens or creates the partial file for a download, sizing it to the
// manifest. existed reports whether a non-empty file was already present, in
// which case the caller should Recheck it.
func Create(partPath, finalPath string, m *manifest.Manifest) (s *Store, existed bool, err error) {
	f, err := os.OpenFile(partPath, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, false, err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, false, err
	}
	if fi.Size() != m.Size() {
		if err := f.Truncate(m.Size()); err != nil {
			f.Close()
			return nil, false, fmt.Errorf("store: sizing %s: %w", partPath, err)
		}
	}
	return &Store{f: f, m: m, path: partPath, finalPath: finalPath, writable: true}, fi.Size() > 0, nil
}

// OpenComplete opens an existing file read-only for seeding.
func OpenComplete(path string, m *manifest.Manifest) (*Store, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if fi.Size() != m.Size() {
		f.Close()
		return nil, fmt.Errorf("store: %s is %d bytes, manifest says %d", path, fi.Size(), m.Size())
	}
	return &Store{f: f, m: m, path: path, finalPath: path}, nil
}

func (s *Store) Path() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.path
}

// Writable reports whether pieces can be written (a download in progress).
func (s *Store) Writable() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.writable
}

func (s *Store) ReadPiece(i int) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.f == nil {
		return nil, os.ErrClosed
	}
	buf := make([]byte, s.m.PieceLen(i))
	n, err := s.f.ReadAt(buf, s.m.PieceOffset(i))
	if n == len(buf) {
		return buf, nil
	}
	if err == nil || err == io.EOF {
		err = io.ErrUnexpectedEOF
	}
	return nil, fmt.Errorf("store: reading piece %d: %w", i, err)
}

// WritePiece writes piece i and flushes it to stable storage before returning
// (fsync on Linux, FlushFileBuffers on Windows). A piece must only be recorded
// as persisted after WritePiece returns nil.
func (s *Store) WritePiece(i int, data []byte) error {
	if len(data) != s.m.PieceLen(i) {
		return fmt.Errorf("store: piece %d is %d bytes, want %d", i, len(data), s.m.PieceLen(i))
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.f == nil {
		return os.ErrClosed
	}
	if !s.writable {
		return ErrReadOnly
	}
	if _, err := s.f.WriteAt(data, s.m.PieceOffset(i)); err != nil {
		return fmt.Errorf("store: writing piece %d: %w", i, err)
	}
	if err := s.f.Sync(); err != nil {
		return fmt.Errorf("store: syncing piece %d: %w", i, err)
	}
	return nil
}

// Finalize renames the completed partial file to its final path and reopens it
// read-only. It is a no-op for stores that are already read-only.
func (s *Store) Finalize() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.writable || s.f == nil {
		return nil
	}
	if err := s.f.Sync(); err != nil {
		return err
	}
	if err := s.f.Close(); err != nil {
		return err
	}
	s.f = nil
	if err := os.Rename(s.path, s.finalPath); err != nil {
		return fmt.Errorf("store: finalizing: %w", err)
	}
	f, err := os.Open(s.finalPath)
	if err != nil {
		return err
	}
	s.f, s.path, s.writable = f, s.finalPath, false
	return nil
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f == nil {
		return nil
	}
	err := s.f.Close()
	s.f = nil
	return err
}

// Recheck hashes every piece on disk and returns the set that matches the manifest.
func (s *Store) Recheck(ctx context.Context, workers int) (bitfield.Bitfield, error) {
	all := make([]int, s.m.NumPieces())
	for i := range all {
		all[i] = i
	}
	return s.Verify(ctx, workers, all)
}

// Verify hashes the given pieces and returns the subset that matches the manifest.
func (s *Store) Verify(ctx context.Context, workers int, pieces []int) (bitfield.Bitfield, error) {
	have := bitfield.New(s.m.NumPieces())
	var (
		mu       sync.Mutex
		wg       sync.WaitGroup
		firstErr error
	)
	next := make(chan int)
	for range max(workers, 1) {
		wg.Go(func() {
			for i := range next {
				data, err := s.ReadPiece(i)
				mu.Lock()
				if err != nil && firstErr == nil {
					firstErr = err
				} else if err == nil && s.m.Verify(i, data) {
					have.Set(i)
				}
				mu.Unlock()
			}
		})
	}
	for _, i := range pieces {
		if ctx.Err() != nil {
			break
		}
		next <- i
	}
	close(next)
	wg.Wait()
	if firstErr == nil {
		firstErr = ctx.Err()
	}
	return have, firstErr
}
