# Single-host throughput ceiling (Phase 7)

All numbers were measured on 2026-10-25 on the development laptop: Windows 11, 16 logical CPUs,
NTFS on both C: and D:, Go 1.27.0. The benchmarks are in the repository and can be re-run:
`go test ./internal/store -bench .`, `go test ./internal/statedb -bench .` and
`go test ./internal/node -run '^$' -bench Transfer`.

## A correction to earlier reports

Phases 2–4 reported "about 50–60 MiB/s per downloader" on loopback. **That figure was wrong.**
- It was measured from downloader process start, so it included waiting for a seeder that was
  still starting up (hashing its file before announcing itself).
- That startup phase varied from about 0.2 s to several seconds between identical runs. The
  cause is not established; antivirus scanning of the freshly regenerated test file is
  suspected.

Measured from the first peer connection, a downloader transfers at **150–164 MiB/s**. Status
now records `first_peer_at`, and the completion log reports `transfer` separately from
`elapsed`.

## Where the time goes (256 KiB pieces)

| operation | per piece | throughput |
|---|---|---|
| SHA-256 (`crypto/sha256`) | 0.12 ms | 2.1 GB/s |
| positioned write, no flush | 0.07 ms | ~4 GB/s |
| positioned write + flush (`FlushFileBuffers`) | 0.9–1.1 ms | 230–290 MB/s |
| same, 2–4 workers in parallel on one file | — | 175–270 MB/s (no gain) |
| SQLite record (WAL, `synchronous=FULL`) | 0.8 ms on C:, 1.6 ms on D: | — |

End to end, 128 MiB from one seeder to one downloader in one process over loopback:

| configuration | MB/s |
|---|---|
| no state DB, 256 KiB pieces | 161–174 |
| no state DB, 1 MiB pieces | 204–273 |
| state DB (as in `swarmflow get`), 256 KiB pieces | 127 |
| state DB, 1 MiB pieces | 237 |
| request pipeline depth 4 vs 16 per peer | no difference (174 vs 174) |

## Conclusion

The ceiling is the **two flushes per piece**, one for the data and one for the durable SQLite
record. That ordering is what makes the crash guarantee hold (see
[durability.md](../durability.md)). Network pipelining is not the limit on loopback.

Larger pieces amortise the flushes: 1 MiB pieces reach about 240 MB/s with the state database.
Group-committing piece records, one transaction for several pieces, would be another option.
Neither is needed for the network experiments, whose links are 2–100 Mbit/s (≤ 12.5 MB/s),
more than 10× below this ceiling. They are noted as future optimisations, not done.
