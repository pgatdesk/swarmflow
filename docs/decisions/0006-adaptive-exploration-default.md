# 0006 — Adaptive selector: exploration off by default

Date: 2026-10-25. Status: **Approved by project owner** (option 1 of 3).

## Problem

The approved design (architecture §7) sent 1 in 10 adaptive decisions to the least-sampled
peer. In the Phase 5 netem experiments ([peer-selection.md](../results/peer-selection.md),
5 trials per variant), this made adaptive 20–40% slower than with exploration off, in all four
scenarios. Pieces kept going to 2–5 Mbit/s seeds, each taking about a second, inside downloads
of 5–10 s.

Limiting exploration to peers with fewer than 3 samples reduced the cost, but did not remove
it.

## Options

1. **Exploration off by default; per-peer cap unchanged at 4 (chosen).**
2. Exploration off, and per-peer cap 16 with upload slots raised to match. This was the best
   single result (4.0 s vs 4.6 s), but it was measured in one scenario only, and it interacts
   with upload slots (about 70 busy rejects per downloader).
3. Keep ε = 0.1.

## Decision

- `scheduler.DefaultExplore = 0`.
- New peers are still measured: an unsampled peer is assumed to have the median measured
  throughput, so it competes as a typical peer.
- `-explore` and `-explore-new-only` remain available for experiments.
- The per-peer cap and upload-slot defaults are to be evaluated across scenarios in Phase 7
  and brought back as a separate decision.

## Consequences

- `swarmlab sim` results were regenerated with the new default.
- The variant named `adaptive` in the Phase 5 result files was run with ε = 0.1, the default
  at that time. Its ε = 0 counterpart is `adaptive-noexplore`.
- Exploration's possible benefit, noticing a peer that has become faster in a long-running
  swarm, is not exercised by the default. Revisit if long-lived swarms show slow adaptation.
