# Integrity, durability and availability in SwarmFlow

These three properties are easy to conflate, so this page states what each one means here, what
provides it, and where it stops.

## Integrity: "the bytes are the right bytes"

- The trusted `.swarm` manifest lists a SHA-256 for every piece. The swarm ID is the SHA-256 of
  the manifest's canonical encoding, so peers cannot alter a manifest without changing the ID.
- Every received piece is hashed before it is written. Pieces with a wrong hash or wrong length
  are rejected and fetched again. A peer that sends two invalid pieces within 10 minutes is
  banned.
- On restart, persisted pieces are re-hashed by default (`-resume-verify full`), which also
  catches corruption that happened on disk between runs.
- **Limit:** integrity is only as trustworthy as the manifest. SwarmFlow does not sign
  manifests or authenticate peers (decision 0001 #9).

## Durability: "a piece we said we have survives a crash"

- **Write order:** the piece's data is written at its offset in `<name>.part`, and
  `File.Sync()` runs (`FlushFileBuffers` on Windows, `fsync` on Linux). Only then is a row
  committed in SQLite, which runs in WAL mode with `synchronous=FULL`.
- **Crash before the commit:** the piece is downloaded again. The crash cost some work, but
  nothing is corrupted.
- **Crash after the commit:** the data had already been flushed to disk.
- **Tested:** `internal/cli` `TestCrashAndResume` hard-kills a real downloader process
  mid-transfer. Every recorded piece was then intact on disk, and a resume fetched exactly the
  missing pieces.
- **Limits:**
  - SwarmFlow relies on the OS and the drive honouring flushes; a drive with a volatile write
    cache that ignores flush requests can lose recent writes on power loss.
  - SQLite and the data file are separate files, so a damaged file system can break the link
    between them. That is why resume re-hashes by default.

## Availability: "some peer can still serve every piece"

- A downloader can finish only while every piece it needs is held by some connected peer.
  `unavailable_pieces` in the status reports how many are not. The download then waits and
  keeps re-discovering through the tracker, PEX and remembered peers; it does not fail.
- **Replication** (`-replicas R`) keeps R−1 *verified* copies of each piece on other peers:
  - Copies are verified with possession proofs. A claimed copy (BITFIELD/HAVE) does not count
    until the peer proves it holds the bytes, and lying peers are detected and banned.
  - Copies lost to disconnects are re-created on other acceptors. In
    `TestReplicationReachesTargetAndRecoversFromLoss`, 21 lost copies were replaced with
    exactly 21 offers.
- **Limits:**
  - Replication is best effort, with no consensus. Placement happens only while the
    replicating peer is online and enough acceptors with budget are connected.
  - **Replicas on the same machine are not machine-level fault tolerance.** Peer processes on
    one computer share its disk, power supply and network. Meaningful fault tolerance needs
    replicas on separate machines (planned: the second-machine run in Phase 7).
  - A replica's verified status is not persisted on the replicator. After a restart, copies
    are re-verified by new challenges.
