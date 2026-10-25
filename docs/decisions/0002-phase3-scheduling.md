# 0002 — Phase 3 scheduling implementation choices

Date: 2026-10-25. Status: **minor, reversible choices made within the approved design**
(0001 #14). Each is documented here per the escalation protocol.

| # | Choice | Rationale |
|---|--------|-----------|
| 1 | Rarest-first tie-break: a seeded permutation; the CLI derives the seed from the random peer ID unless `-seed` is given | Same seed → deterministic tests; different peers → different tie orders, so downloaders do not all request the same equally rare piece |
| 2 | Rarest-first runs in O(pieces + max availability) using availability buckets | Avoids a sort on every scheduling event |
| 3 | Throughput sample = piece bytes ÷ time since max(request sent, previous piece from that peer arrived) | With pipelined requests, per-request latency includes queueing at the remote; inter-arrival time approximates the rate the peer sustains to us |
| 4 | Per-peer stats are kept across reconnects (pruned after 30 min idle, max 1024) | A peer cannot shed a bad reputation by reconnecting with the same ID. Peer IDs are random per process, so a restarted process starts fresh (documented limitation) |
| 5 | A peer in cooldown is still used when it is the only source of a piece, one request at a time | "A slow peer should not automatically be treated as dead" — probing keeps progress possible |
| 6 | `REJECT(not_have)` lowers reliability like a timeout | Superseded for `REJECT(busy)` by [0003](0003-upload-admission-cap.md): busy is backpressure, not failure |
| 7 | Endgame duplicates only overdue requests (age ≥ 1.5 × expected and ≥ 200 ms), max 2 copies per piece; first persisted copy wins, others are cancelled | Spends extra bandwidth only where a slow peer is actually delaying completion |
| 8 | Coordinator tick 100 ms (timeouts, pings, endgame polling); status snapshot every 250 ms | Bounds timeout lateness to 100 ms |
| 9 | Defaults changed to `-picker rarest -selector adaptive` | The approved design's defaults; sequential and random remain available as baselines |
