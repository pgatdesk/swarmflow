# SwarmFlow benchmark report (Phase 7)

**What was measured:** real `swarmflow` processes, one Linux network namespace per peer, with
each peer's access link shaped by `tc netem` in both directions (delay, jitter, rate, loss;
decision 0005). Failures were injected by the lab as SIGKILL, graceful stop, link down/up and
restart.

**Run:** 2026-10-26, WSL2 kernel 6.18 on a 16-CPU Windows 11 laptop, Go 1.27.0. 12 required
scenarios plus 1 extra, 5 trials per variant, variants interleaved: **100 trials, 380
downloader runs, 0 trial errors.**

**Reproduce:** `./scripts/lab/run-required.ps1 -Trials 5`. Per-scenario tables (peers, links,
events, sources, failures, measured durations) and raw per-trial JSON are in
[required/](required/) and [extra/](extra/).

**How to read the numbers:**
- Values are medians across trials, with [p25–p75] where useful.
- **Completion time** runs from the downloader's first process start to its last piece being
  persisted, so it includes restarts and any wait for a source.
- **Detection time** runs from the lab's failure injection to the observer's own
  disconnect or timeout event.
- These are emulated networks on one host: all processes share one CPU and disk, so timings
  are internally comparable but are not internet measurements.

## Required scenarios

| # | scenario (brief §8) | key result (median) | verdict |
|---|---|---|---|
| 1 | One seeder, one downloader (50 Mbit/s seed) | 64 MiB in **11.5 s** = 5.55 MiB/s, 93% of the 6.25 MB/s link | pass |
| 2 | One seeder, 8 downloaders joining over 6 s (40 Mbit/s seed, 20 Mbit/s downloaders) | rarest-first **31.8 s**, random 31.4 s, sequential **64.2 s**; downloaders traded pieces with each other | pass; sequential is 2× slower, rarest-first ≈ random order here |
| 3 | Three partial seeders (even, odd, first half), no full seed | **11.8 s**; every download complete and verified | pass |
| 4 | Fastest of three seeders SIGKILLed mid-transfer | **16.5 s**; peers detected the death in **0.24–0.26 s**; 7 requests reassigned | pass |
| 5 | Downloader SIGKILLed at 6 s, restarted at 9 s | crashed downloader finished in **10.1 s** from its first start, about the same as the uninterrupted one; resume re-verification **22 ms** | pass |
| 6 | 10% of a fast seeder's pieces corrupted | all corrupted pieces rejected (2 per downloader), liar **banned** by both downloaders, files byte-identical; **27.1 s**, because the honest seeder is 2× slower | pass (ban costs throughput, see below) |
| 7 | 1 Mbit/s, 150 ms seeder next to 50 and 20 Mbit/s | **endgame 8.6 s vs 15.6 s without (−45%)** | pass |
| 8 | High RTT (~400 ms) and 40 Mbit/s seeders; request depth 4/8/16 | depth 4 **10.7 s**, depth 8 11.7 s, depth 16 13.6 s; first piece after 3.5 s (TCP slow start at high RTT) | pass; deeper pipelines did not help (decision 0007) |
| 9 | 1–2% loss, jitter, and a seeder link down twice for 2 s | **47.1 s** (TCP goodput under loss); outages detected by request timeout in 1.8–2.1 s | pass |
| 10 | Replica holder killed; original seeder leaves before the downloader joins | target copies **restored 2.98 s** after the kill; the downloader completed from replicas alone in **1.9 s** | pass |
| 11 | Six downloaders compete for two 20 Mbit/s seeders | **20.9 s**; downloader fairness (Jain, throughput) **1.00**; upload cap on/off identical | pass |
| 12 | No source at start; seeder appears after 8 s | first piece **8.3 s** after start (0.3 s after the seeder appeared); complete in 22.6 s | pass |

**Resources:**
- Peak RSS per downloader was **19–28 MiB** in every scenario, measured from each process's own
  VmHWM.
- CPU time was 0.3–2.0 s per downloader run.

## Extra: early seed departure (validates decision 0003)

**Setup:** one 40 Mbit/s seed and 8 downloaders with 10 Mbit/s uplinks. The seed leaves after
20 s.

**Result: 0 of 80 downloader runs completed, with or without the upload cap.**
- The seed had uploaded 1.1–1.2 copies before leaving.
- Downloaders shared everything they had, so in each trial all of them ended with the same
  213–227 of 256 pieces: the swarm held only **84–89%** of the file.
- The cap never engaged (0 busy rejects).
- The cause is **duplicate seed uploads**. Independent downloaders request the same rare piece
  before any of them announces it, so about 1 in 6 seed uploads was a duplicate, even though
  1.2 copies would be enough to cover every piece.
- The simulator predicted the cap would fix this case. It did not, because the simulator's
  mechanism (a deep, stale seed queue) does not occur here. See decision 0003, and the open
  decision 0007 on seed-side piece distribution.

## Measured effects of policies (all on real emulated networks)

| policy | effect | where |
|---|---|---|
| Endgame duplicates for overdue pieces | −45% completion with a slow peer present | 07 |
| Measurement-based peer selection vs random | −35–48% completion | Phase 5 selection scenarios |
| Adaptive vs fastest-peer selection | −13% under churn; −22% at request depth 16; tie otherwise | Phase 5 |
| Rarest-first vs random piece order | no measurable difference (31.8 vs 31.4 s) | 02 |
| Rarest-first vs sequential | −50% | 02 |
| Request depth 8 or 16 vs 4 at ~400 ms RTT | +9% and +27% slower | 08 |
| Upload admission cap (16) vs none | no measurable difference; it never engaged | 02, 11, 13 |
| Ban after 2 invalid pieces | safe; also drops a fast seeder whose data was 90% good | 06 |

## Things that did not go as designed (kept in the report on purpose)

1. **Early seed departure loses 11–16% of the file** (extra 13), on real networks as in
   simulation, and the upload cap does not fix it. Open: decision 0007.
2. **Request depth.** The Phase 5 hypothesis that deeper pipelines help at high RTT did not
   hold: they were slower, because more requests queue behind TCP's congestion window and
   spread load less evenly.
3. **Measurement corrections made during Phase 7:**
   - Throughput was earlier misreported, because time spent waiting for the seeder's startup
     was counted as transfer time ([throughput.md](throughput.md)).
   - Peak RSS was inflated by counting the lab's own memory, which children inherit at fork.
   - Both are fixed. The numbers above use the corrected measurements.
4. **Failure detection depends on how a peer fails.**
   - A killed or stopped peer is noticed within about 0.25 s, plus the time to drain the
     netem queue (1–2.7 s with a 1000-packet queue on 10 Mbit/s links in extra 13).
   - A silent link outage is noticed only by request timeout (at least 2 s).

## Limitations

- **Single-host emulation.** It does not show machine-level fault tolerance; the two-machine run
  ([two-machine.md](../two-machine.md)) is planned.
- **Five trials per variant.** Enough for the effects above (10–50%), not for differences of a
  few percent.
- **The emulated links have no cross-traffic or NAT,** and the netem queue length (1000 packets)
  is a single fixed choice.
