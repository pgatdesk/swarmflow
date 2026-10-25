// Package peerstats estimates per-peer performance from observed request
// outcomes. It holds no clocks of its own: every method takes the observation
// time, so the same code runs under the real clock and in the simulator.
//
// Measurements and their caveats:
//   - Throughput is payload delivery rate: piece bytes divided by the time since
//     the later of (this request was sent, the previous piece from this peer
//     arrived). With pipelined requests this approximates the rate the peer can
//     sustain to us, not the link capacity.
//   - RTT comes from PING/PONG on the data connection, so it includes queueing
//     behind data already in flight ("application RTT"). Timeouts never produce
//     RTT or latency samples.
//   - Reliability is an EWMA of request outcomes (success = 1, timeout or
//     reject = 0).
package peerstats

import (
	"slices"
	"time"
)

type Config struct {
	// Alpha is the EWMA weight of a new throughput or RTT sample.
	Alpha float64
	// ReliabilityAlpha is the EWMA weight of a new outcome.
	ReliabilityAlpha float64
	// MinSamples is the number of samples before an estimate is reported as known.
	MinSamples int
	// LatencyWindow is the number of recent request latencies kept for percentiles.
	LatencyWindow int
	// TimeoutsForCooldown consecutive timeouts put the peer in cooldown.
	TimeoutsForCooldown int
	CooldownMin         time.Duration
	CooldownMax         time.Duration
	// InvalidForBan invalid pieces within InvalidWindow ban the peer.
	InvalidForBan int
	InvalidWindow time.Duration
}

func DefaultConfig() Config {
	return Config{
		Alpha:               0.3,
		ReliabilityAlpha:    0.2,
		MinSamples:          3,
		LatencyWindow:       200,
		TimeoutsForCooldown: 3,
		CooldownMin:         5 * time.Second,
		CooldownMax:         80 * time.Second,
		InvalidForBan:       2,
		InvalidWindow:       10 * time.Minute,
	}
}

type Stats struct {
	cfg *Config

	throughput  float64 // bytes/s
	tpSamples   int
	rtt         float64 // seconds
	rttSamples  int
	reliability float64

	lat     []time.Duration // ring buffer
	latNext int
	latN    int
	pcache  [2]time.Duration // p50, p95; valid when !pdirty
	pdirty  bool

	lastArrival    time.Time
	lastSeen       time.Time
	consecTimeouts int
	cooldownUntil  time.Time
	cooldownLen    time.Duration
	invalidAt      []time.Time
	banned         bool

	Successes, Timeouts, Rejects, Invalid int
	Bytes                                 int64
}

// New returns stats for a peer with no history. cfg must outlive the Stats.
func New(cfg *Config) *Stats {
	return &Stats{cfg: cfg, reliability: 1, lat: make([]time.Duration, cfg.LatencyWindow)}
}

func (s *Stats) ewma(old, sample float64, n int) float64 {
	if n == 0 {
		return sample
	}
	return s.cfg.Alpha*sample + (1-s.cfg.Alpha)*old
}

func (s *Stats) outcome(ok bool) {
	v := 0.0
	if ok {
		v = 1
	}
	s.reliability = s.cfg.ReliabilityAlpha*v + (1-s.cfg.ReliabilityAlpha)*s.reliability
}

// OnSuccess records a valid piece of n bytes requested at sent and fully received at now.
func (s *Stats) OnSuccess(now, sent time.Time, n int) {
	s.Successes++
	s.Bytes += int64(n)
	s.lastSeen = now
	s.outcome(true)
	s.consecTimeouts = 0
	s.cooldownLen = 0

	start := sent
	if s.lastArrival.After(start) {
		start = s.lastArrival
	}
	if d := now.Sub(start); d > 0 {
		s.throughput = s.ewma(s.throughput, float64(n)/d.Seconds(), s.tpSamples)
		s.tpSamples++
	}
	s.lastArrival = now

	s.lat[s.latNext] = now.Sub(sent)
	s.latNext = (s.latNext + 1) % len(s.lat)
	s.latN = min(s.latN+1, len(s.lat))
	s.pdirty = true
}

// OnTimeout records a request that missed its deadline.
func (s *Stats) OnTimeout(now time.Time) {
	s.Timeouts++
	s.outcome(false)
	s.consecTimeouts++
	if s.consecTimeouts >= s.cfg.TimeoutsForCooldown {
		s.consecTimeouts = 0
		if s.cooldownLen == 0 {
			s.cooldownLen = s.cfg.CooldownMin
		} else {
			s.cooldownLen = min(2*s.cooldownLen, s.cfg.CooldownMax)
		}
		s.cooldownUntil = now.Add(s.cooldownLen)
	}
}

// OnReject records a REJECT (busy or not-have).
func (s *Stats) OnReject(now time.Time) {
	s.Rejects++
	s.lastSeen = now
	s.outcome(false)
}

// OnInvalid records a piece that failed verification and reports whether the
// peer has now crossed the ban threshold.
func (s *Stats) OnInvalid(now time.Time) bool {
	s.Invalid++
	s.outcome(false)
	s.invalidAt = append(s.invalidAt, now)
	cutoff := now.Add(-s.cfg.InvalidWindow)
	s.invalidAt = slices.DeleteFunc(s.invalidAt, func(t time.Time) bool { return t.Before(cutoff) })
	if len(s.invalidAt) >= s.cfg.InvalidForBan {
		s.banned = true
	}
	return s.banned
}

// OnRTT records a PING/PONG round trip.
func (s *Stats) OnRTT(now time.Time, d time.Duration) {
	s.lastSeen = now
	s.rtt = s.ewma(s.rtt, d.Seconds(), s.rttSamples)
	s.rttSamples++
}

func (s *Stats) Throughput() (bytesPerSec float64, ok bool) {
	return s.throughput, s.tpSamples >= s.cfg.MinSamples
}

func (s *Stats) RTT() (time.Duration, bool) {
	return time.Duration(s.rtt * float64(time.Second)), s.rttSamples >= 1
}

func (s *Stats) Reliability() float64          { return s.reliability }
func (s *Stats) Banned() bool                  { return s.banned }
func (s *Stats) LastSeen() time.Time           { return s.lastSeen }
func (s *Stats) CooldownUntil() time.Time      { return s.cooldownUntil }
func (s *Stats) InCooldown(now time.Time) bool { return now.Before(s.cooldownUntil) }

// Latency returns the p50 and p95 of recent request latencies.
func (s *Stats) Latency() (p50, p95 time.Duration, ok bool) {
	if s.latN < s.cfg.MinSamples {
		return 0, 0, false
	}
	if s.pdirty {
		w := slices.Clone(s.lat[:s.latN])
		slices.Sort(w)
		s.pcache = [2]time.Duration{quantile(w, 0.50), quantile(w, 0.95)}
		s.pdirty = false
	}
	return s.pcache[0], s.pcache[1], true
}

// quantile uses the nearest-rank method on sorted samples.
func quantile(sorted []time.Duration, q float64) time.Duration {
	i := int(q*float64(len(sorted))+0.999999) - 1
	return sorted[max(0, min(i, len(sorted)-1))]
}
