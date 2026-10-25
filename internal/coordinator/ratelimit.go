package coordinator

import (
	"context"
	"sync"
	"time"
)

// rateLimiter is a token bucket shared by a swarm's uploaders. Tokens are
// bytes; the bucket holds at most one second of tokens, so after an idle period
// the limiter allows a short burst and then settles at the configured rate.
type rateLimiter struct {
	mu     sync.Mutex
	rate   float64 // bytes per second
	tokens float64
	last   time.Time
}

func newRateLimiter(bytesPerSec float64) *rateLimiter {
	if bytesPerSec <= 0 {
		return nil
	}
	return &rateLimiter{rate: bytesPerSec, tokens: bytesPerSec, last: time.Now()}
}

// wait blocks until n bytes may be sent. A nil limiter never blocks.
func (l *rateLimiter) wait(ctx context.Context, n int) error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	now := time.Now()
	l.tokens = min(l.tokens+now.Sub(l.last).Seconds()*l.rate, l.rate)
	l.last = now
	l.tokens -= float64(n) // may go negative: later callers wait for the debt to clear
	debt := -l.tokens
	l.mu.Unlock()
	if debt <= 0 {
		return nil
	}
	t := time.NewTimer(time.Duration(debt / l.rate * float64(time.Second)))
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
