package peerstats

import (
	"math"
	"testing"
	"time"
)

var t0 = time.Unix(1000, 0)

func at(d time.Duration) time.Time { return t0.Add(d) }

func TestThroughputUsesPipelinedArrivals(t *testing.T) {
	cfg := DefaultConfig()
	s := New(&cfg)
	// Four requests sent at t=0, pieces of 100 KB arriving every 100 ms: the
	// peer delivers 1 MB/s even though each request's latency grows.
	for i := 1; i <= 4; i++ {
		s.OnSuccess(at(time.Duration(i)*100*time.Millisecond), t0, 100_000)
	}
	tp, ok := s.Throughput()
	if !ok || math.Abs(tp-1e6) > 1 {
		t.Fatalf("throughput %.0f ok=%v, want 1e6", tp, ok)
	}
	p50, p95, ok := s.Latency()
	if !ok || p50 != 200*time.Millisecond || p95 != 400*time.Millisecond {
		t.Fatalf("latency p50=%v p95=%v", p50, p95)
	}
}

func TestColdStartUnknown(t *testing.T) {
	cfg := DefaultConfig()
	s := New(&cfg)
	if _, ok := s.Throughput(); ok {
		t.Fatal("throughput known with no samples")
	}
	s.OnSuccess(at(time.Second), t0, 1000)
	if _, ok := s.Throughput(); ok {
		t.Fatal("throughput known after 1 of 3 samples")
	}
	if _, _, ok := s.Latency(); ok {
		t.Fatal("latency known after 1 sample")
	}
}

func TestEWMAWeighting(t *testing.T) {
	cfg := DefaultConfig()
	s := New(&cfg)
	s.OnRTT(t0, 100*time.Millisecond)
	s.OnRTT(t0, 200*time.Millisecond)
	got, _ := s.RTT()
	want := time.Duration(0.3*200e6 + 0.7*100e6)
	if d := got - want; d < -time.Microsecond || d > time.Microsecond {
		t.Fatalf("rtt %v, want %v", got, want)
	}
}

func TestCooldownBackoffAndReset(t *testing.T) {
	cfg := DefaultConfig()
	s := New(&cfg)
	timeouts := func(now time.Time) {
		for range cfg.TimeoutsForCooldown {
			s.OnTimeout(now)
		}
	}
	timeouts(t0)
	if !s.InCooldown(at(4*time.Second)) || s.InCooldown(at(5*time.Second)) {
		t.Fatalf("first cooldown should last 5s, until %v", s.CooldownUntil().Sub(t0))
	}
	timeouts(at(10 * time.Second))
	if got := s.CooldownUntil().Sub(at(10 * time.Second)); got != 10*time.Second {
		t.Fatalf("second cooldown %v, want 10s", got)
	}
	for range 10 {
		timeouts(at(time.Hour))
	}
	if got := s.CooldownUntil().Sub(at(time.Hour)); got != cfg.CooldownMax {
		t.Fatalf("cooldown not capped: %v", got)
	}
	s.OnSuccess(at(2*time.Hour), at(2*time.Hour-time.Second), 1)
	timeouts(at(3 * time.Hour))
	if got := s.CooldownUntil().Sub(at(3 * time.Hour)); got != cfg.CooldownMin {
		t.Fatalf("cooldown not reset by success: %v", got)
	}
}

func TestTwoTimeoutsThenSuccessNoCooldown(t *testing.T) {
	cfg := DefaultConfig()
	s := New(&cfg)
	s.OnTimeout(t0)
	s.OnTimeout(t0)
	s.OnSuccess(at(time.Second), t0, 1)
	s.OnTimeout(at(2 * time.Second))
	if s.InCooldown(at(2 * time.Second)) {
		t.Fatal("non-consecutive timeouts triggered cooldown")
	}
}

func TestBanWithinWindow(t *testing.T) {
	cfg := DefaultConfig()
	s := New(&cfg)
	if s.OnInvalid(t0) {
		t.Fatal("banned after one invalid piece")
	}
	if s.OnInvalid(at(cfg.InvalidWindow + time.Second)) {
		t.Fatal("banned for invalid pieces outside the window")
	}
	if !s.OnInvalid(at(cfg.InvalidWindow + 2*time.Second)) {
		t.Fatal("not banned for two invalid pieces within the window")
	}
}

func TestReliability(t *testing.T) {
	cfg := DefaultConfig()
	s := New(&cfg)
	if s.Reliability() != 1 {
		t.Fatal("new peers start fully reliable")
	}
	s.OnTimeout(t0)
	if math.Abs(s.Reliability()-0.8) > 1e-9 {
		t.Fatalf("reliability %v after one failure, want 0.8", s.Reliability())
	}
	s.OnSuccess(at(time.Second), t0, 1)
	if math.Abs(s.Reliability()-0.84) > 1e-9 {
		t.Fatalf("reliability %v, want 0.84", s.Reliability())
	}
}
