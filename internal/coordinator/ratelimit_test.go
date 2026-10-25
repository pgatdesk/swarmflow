package coordinator

import (
	"context"
	"testing"
	"time"
)

func TestRateLimiterSettlesAtRate(t *testing.T) {
	l := newRateLimiter(1 << 20) // 1 MiB/s, with a 1 MiB initial burst
	ctx := context.Background()
	start := time.Now()
	for range 12 { // 3 MiB in 256 KiB chunks: 1 MiB burst + 2 MiB at 1 MiB/s ≈ 2 s
		if err := l.wait(ctx, 256<<10); err != nil {
			t.Fatal(err)
		}
	}
	if el := time.Since(start); el < 1800*time.Millisecond || el > 2600*time.Millisecond {
		t.Fatalf("3 MiB took %v; want ~2 s at 1 MiB/s after a 1 MiB burst", el)
	}
	var nilLimiter *rateLimiter
	if err := nilLimiter.wait(ctx, 1<<30); err != nil {
		t.Fatal("nil limiter blocked")
	}
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if err := l.wait(cctx, 10<<20); err == nil {
		t.Fatal("cancelled wait returned nil")
	}
}
