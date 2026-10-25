# 0004 — Phase 4 fault-tolerance implementation choices

Date: 2026-10-25. Status: **minor, reversible choices made within the approved design**
(architecture §6, §9 and §10; decisions 0001 #6 and #14).

| # | Choice | Rationale |
|---|--------|-----------|
| 1 | Only the Persisted state is durable. A `pieces` row is written after `WritePiece` (write + flush) returns, in the verify worker | A row always refers to flushed data. A crash between flush and commit only costs a re-download |
| 2 | One SQLite file per `get`/`replica` process (`<out dir>/.swarmflow/state.db`, `-state-db`). WAL mode, `synchronous=FULL`, a single connection | Matches the one-swarm-per-process CLI. WAL lets tests and tools read while the peer writes |
| 3 | Resume re-hashes recorded pieces by default (`-resume-verify full`). `trust` skips hashing. Unrecorded data in `.part` is ignored. A `.part` with no record is fully re-hashed and adopted | Catches media corruption between runs, never trusts possibly torn writes, and stays compatible with files from before the database existed |
| 4 | The CLI moved to `internal/cli`, and the crash test re-runs the test binary as the child process | Child binaries built into the Windows temp folder could be refused sockets (see Troubleshooting); the test binary itself is built under `GOTMPDIR` |
| 5 | PEX: on connect and every 30 s, up to 50 shuffled listen addresses of other connected peers. Inbound PEX is acted on at most once per 10 s per peer. `-pex-interval 0` disables it | Re-discovery without the tracker; bounded amplification |
| 6 | Peers that completed a handshake are stored in `peers_seen`. On restart, those seen in the last 24 h are dialed only while the swarm has no peers | Re-discovery after a restart with no tracker, without redialing old addresses during normal operation |
| 7 | Replication acceptors advertise handshake flag `0x0001`. `swarmflow replica` accepts offers up to `-replica-budget` and fetches only accepted pieces, through the normal verified path | A replica is only ever created from hash-checked data, and placement needs no new transfer mechanism |
| 8 | A replica counts only after a `PROOF` of SHA-256(nonce ‖ piece). Proofs expire after 5 min and are dropped when the holder disconnects. A wrong proof counts as an invalid piece (two lead to a ban); a proof timeout only means "not verified" | Distinguishes claimed availability from verified copies. An overloaded peer is not punished as a liar |
| 9 | Each sweep (every 500 ms) per held piece: challenge unverified claimants, then offer to acceptors, rotating the start peer by piece index. Pending challenges and offers count toward the target. At most 32 of each in flight | No overshoot from a single replicator; load spreads across acceptors |
| 10 | Pieces with no connected source are reported as `unavailable_pieces` and logged. The download waits and keeps searching instead of failing | "What happens when all copies disappear" (brief §5): progress stalls visibly and resumes when a source reappears |
| 11 | Proof work runs on one worker goroutine with a 64-entry queue. Challenge answers are dropped when it is full | Bounds the disk reads a peer can trigger by sending challenges |

## Known costs and limits

- **Proofs need re-checking.** Each verified copy needs a new proof every 5 minutes, and each
  proof reads the whole piece on both sides. For 1,024 pieces × 2 copies, that is about 7
  challenges per second.
- **Batched proofs would be cheaper.** A single proof covering many pieces, for example a
  Merkle-style proof, is future work.
- **No coordination between replicators.** Several holders with `-replicas` may replicate the
  same piece at once and overshoot the target. With a single replicator the tests measured no
  overshoot.
