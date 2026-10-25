// Package verify runs piece hashing and the durable write off the coordinator's
// goroutine, so a slow disk or CPU never stalls protocol handling.
package verify

import (
	"context"
	"sync"
	"time"

	"github.com/pgatdesk/swarmflow/internal/manifest"
	"github.com/pgatdesk/swarmflow/internal/protocol"
)

// Committer durably stores a piece that has passed verification.
type Committer func(piece int, data []byte) error

type Job struct {
	Piece int
	Data  []byte
	From  protocol.PeerID
	// Latency is the time from REQUEST sent to PIECE fully received.
	Latency time.Duration
}

type Result struct {
	Piece   int
	Size    int
	From    protocol.PeerID
	Latency time.Duration
	// Valid is false when the data did not match the manifest hash.
	Valid bool
	// Err is a commit (storage) failure for valid data.
	Err error
}

type Pool struct {
	m       *manifest.Manifest
	commit  Committer
	workers int
	jobs    chan Job
	results chan Result
}

// NewPool creates a pool whose job and result queues hold queue entries.
func NewPool(m *manifest.Manifest, commit Committer, workers, queue int) *Pool {
	return &Pool{
		m:       m,
		commit:  commit,
		workers: max(workers, 1),
		jobs:    make(chan Job, queue),
		results: make(chan Result, queue),
	}
}

// Submit enqueues a job without blocking; it reports false if the queue is full.
func (p *Pool) Submit(j Job) bool {
	select {
	case p.jobs <- j:
		return true
	default:
		return false
	}
}

func (p *Pool) Results() <-chan Result { return p.results }

// Run processes jobs until ctx is done and all workers have exited.
func (p *Pool) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for range p.workers {
		wg.Go(func() {
			for {
				select {
				case <-ctx.Done():
					return
				case j := <-p.jobs:
					r := Result{Piece: j.Piece, Size: len(j.Data), From: j.From, Latency: j.Latency}
					if p.m.Verify(j.Piece, j.Data) {
						r.Valid = true
						r.Err = p.commit(j.Piece, j.Data)
					}
					select {
					case p.results <- r:
					case <-ctx.Done():
						return
					}
				}
			}
		})
	}
	wg.Wait()
}
