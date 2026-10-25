# 0003 — Swarm-wide upload admission cap with busy back-off

Date: 2026-10-25. Status: **Approved by project owner** (option 1 of 3).

## Problem

In simulation (`swarmlab sim`, scenario `seed-departs`), when the only seed left after uploading
~1.5 copies, **no downloader completed under any picker**. Using rarest-first, only 246/256 pieces
were still held by an online peer.

Cause: 20 downloaders × 4 requests per peer put ~80 requests in the seed's FIFO queue. Each
piece was chosen when the request was sent, but served seconds later, so the rarity information
behind it was stale.

The same queueing caused ~700 request timeouts per flash-crowd run.

## Options considered

1. **Swarm-wide cap on queued + in-progress uploads** (default 16). Requests beyond it get
   `REJECT(busy)`. Busy rejects do not lower reliability; the requester avoids that peer for
   250 ms ± 25 %, then picks again with fresher availability. No protocol change.
2. **BitTorrent-style choking** (upload to k peers at a time, with rotation and reciprocity).
   Needs new protocol messages and much more work.
3. **No change**: document the weakness.

## Decision

Option 1.

- `coordinator.Config.UploadQueueCap` (CLI `-upload-queue`, 0 disables) and `BusyBackoff`.
- `peerstats.OnBusy` records busy rejects without changing reliability. This supersedes 0002 #6
  for busy rejects; `REJECT(not_have)` still counts as a failure.

## Evidence (simulated, 20 trials, median)

| scenario | variant | without cap | with cap 16 |
|---|---|---|---|
| seed-departs | rarest/random | 0 % completed, 96.1 % of pieces survive | **100 % completed**, 100 % survive |
| seed-departs | random/random | 0 % completed | 0 % completed (random order cannot target rare pieces) |
| flash-crowd | rarest/adaptive | 48.1 s mean completion, 574 timeouts | 45.4 s, 0 timeouts |

Full tables: [docs/results/sim-schedulers.md](../results/sim-schedulers.md).

## Real-network validation (2026-10-26): the simulated benefit was not reproduced

The cap was tested in the netem lab: scenarios 02, 11 and the extra 13-seed-departs, 70 trials
in total ([benchmark report](../results/benchmark-report.md)).

**It almost never engaged: one busy reject in total.** Seed upload queues stayed below 16
because each downloader also had other peers to fetch from. With and without the cap, results
were identical within noise.

**In 13-seed-departs, which mirrors the simulated case, no downloader finished in either
variant:**
- The only seed left after uploading 1.1–1.2 copies.
- Downloaders then shared everything among themselves, but the swarm held only 84–89% of the
  pieces.

**The cause is not queue staleness, which the cap addresses.** It is duplicate seed uploads:
several downloaders request the same rare piece before anyone announces it, so about 1 in 6
seed uploads was a duplicate.

The cap is kept, because it is harmless and bounds seed queues by construction. The
early-departure weakness is open; see the Phase 7 report and decision 0007.

## Consequences

- One more tuning parameter, plus extra REJECT/retry traffic (measured: 121 busy rejects for
  4 downloaders × 512 KiB with a cap of 2 in the integration test).
- The cap is per swarm. A process seeding several swarms has one cap per swarm, not a global one.
