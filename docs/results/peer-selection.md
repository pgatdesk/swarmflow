# Peer-selection comparison (Phase 5)

All numbers on this page were measured on 2026-10-25 with real `swarmflow` processes. Each process
ran in its own Linux network namespace inside WSL2, with its access link shaped by `tc netem` in
both directions (decision 0005). The raw per-trial data and the generated tables are in the
per-scenario `.json` and `.md` files next to this page.

## Method

- **Workload:** a 64 MiB file in 256 KiB pieces. Ten seeds with very different links (upload
  2–100 Mbit/s, one-way delay 5–200 ms) serve two downloaders on 400 Mbit/s links. The
  downloaders also trade pieces with each other.
- **What varies:** only the downloaders' `-selector` (and, where named, `-explore` or
  `-max-per-peer`). The picker is rarest-first for every variant.
- **Same conditions for every variant:** identical data, links and policy seeds per trial. 5
  trials per variant, interleaved (trial 1 of every variant, then trial 2, …) so drift on the
  host cannot favour one variant.
- **Reported values:** median and [p25–p75] across trials.
- **Metric:** mean completion is a downloader's time from process start (including connecting)
  to the last piece being persisted, averaged over both downloaders.

## Results: mean completion time (s), median [p25–p75]

| variant | heterogeneous | churn | lossy | flaky |
|---|---|---|---|---|
| random | 7.6 [6.3–7.6] | 9.9 [9.3–10.0] | 11.8 [11.4–13.2] | 8.8 [8.5–8.9] |
| fastest | 5.0 [4.9–5.1] | 6.9 [6.5–6.9] | 7.5 [7.2–7.6] | 5.9 [5.7–6.1] |
| adaptive (default ε = 0.1) | 6.3 [6.0–6.6] | 8.3 [7.8–8.3] | 9.5 [9.2–9.8] | 7.4 [7.3–7.5] |
| adaptive, ε = 0 | **4.6** [4.5–4.7] | **6.0** [5.8–6.0] | 7.3 [7.3–7.5] | 5.9 [5.8–6.0] |
| adaptive, explore new peers only | 6.4 [6.3–6.5] | 7.2 [7.1–7.3] | 8.5 [8.5–8.7] | 6.4 [6.2–7.3] |
| fastest, max-per-peer 16 | 5.1 [5.0–5.8] | — | — | — |
| adaptive ε = 0, max-per-peer 16 | **4.0** [4.0–4.1] | — | — | — |

Scenarios:
- **heterogeneous:** the static workload above.
- **churn:** the fastest seed is killed (SIGKILL) at 3 s, and the second-fastest loses its link
  from 3 s to 6 s.
- **lossy:** the two fastest seeds have 3% and 1% packet loss.
- **flaky:** the fastest seed's link is down for 2.5 s out of every 4 s, starting at 2 s.

All 220 downloader runs completed, and no trial failed.

## What the data shows

1. **Measurement-based selection beats random by 35–48% in every scenario.** Random keeps
   sending pieces to 2–5 Mbit/s seeds, and each such piece takes about a second.
2. **Adaptive with exploration off is never worse than fastest, and is better in two cases:**
   - **Churn:** 6.0 vs 6.9 s (−13%), with non-overlapping ranges.
   - **Per-peer cap raised to 16:** 4.0 vs 5.1 s (−22%). This was the best result overall.
3. **In the lossy and flaky scenarios adaptive and fastest tie.** Both converge to the same
   behaviour for the reasons below.
4. **Exploration as configured by default (10% of decisions) costs 20–40% on these 5–10 s
   downloads.** Limiting exploration to peers with fewer than 3 samples reduces the cost, but
   does not remove it.

## Why adaptive mostly ties fastest, and where it wins

- **The throughput estimate already reflects RTT and loss.** It is a pipelined delivery rate,
  so a high-RTT or lossy peer simply measures slow. In the lossy scenario, both selectors gave
  the 100 Mbit/s seed with 3% loss only 8% of the load, against its 37% capacity share,
  without any loss-specific logic.
- **Health gating applies to every selector.** Cooldown after 3 consecutive timeouts, bans,
  and busy back-off all happen before selection. In the flaky scenario all variants logged
  the same ~4 timeouts per downloader, then avoided the flaky seed. Adaptive's reliability
  term has nothing left to add.
- **The default per-peer cap of 4 already prevents fastest from overloading one peer.** With
  10 seeds and a 32-request budget, fastest fills the best seeds up to the cap, then the next
  best, which is close to capacity-proportional.
- **When the cap no longer constrains, adaptive's load term matters.** At max-per-peer 16:
  - fastest piled 46% of the load on the top seed (37% capacity share);
  - adaptive gave it 36%, almost exactly proportional;
  - and adaptive finished 22% sooner.

  In the churn scenario, adaptive re-spread load faster after the two best seeds failed.

## Caveats

- **Caps interact with the seeds' upload slots.** At max-per-peer 16, downloaders hit about 70
  busy rejects each, because each seed queues only 4 uploads per remote peer (`-upload-slots`).
  The −22% therefore comes from the cap and the upload slots together.
- **Five trials per variant** is enough to separate the large effects (random vs. measured
  selection, exploration cost), but not 2–3% differences. The heterogeneous scenario was run
  twice on different occasions: fastest measured 4.5 then 5.0 s, adaptive ε = 0 measured 4.6
  then 4.6 s.
- **This is single-host emulation.** All 12 processes share one CPU and disk, and the bridge
  itself is unconstrained. Each downloader used about 1–2 CPU-seconds per run.
- **Exploration's cost shrinks with download length.** Its value, finding a peer that has
  become fast, would show in long-running swarms. These short downloads do not test that.
