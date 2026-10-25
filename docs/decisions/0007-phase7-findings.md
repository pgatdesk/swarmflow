# 0007 — Phase 7 findings: request depth, and early seed departure

Date: 2026-10-26. Status: **proposed, awaiting the owner's decision.**

## A. Per-peer request depth (follow-up to 0006)

**Evidence:** scenario 08, high RTT (~400 ms), 40 Mbit/s seeders, 5 trials. Upload slots were
matched to the depth.

| depth | median completion |
|---|---|
| 4 (current default) | 10.7 s |
| 8 | 11.7 s |
| 16 | 13.6 s |

In Phase 5, depth 16 helped only the adaptive selector, and only in combination with busy
rejects.

**Proposal:** keep `-max-per-peer 4` and `-upload-slots 4`. This is no change.

## B. Early seed departure

**Evidence:** extra scenario 13.
- When the only seed leaves after uploading about 1.2 copies, the swarm holds only 84–89% of
  the pieces, so no downloader can finish.
- About 1 in 6 seed uploads duplicated a piece the seed had already sent.
- The upload cap (0003) does not help.

**Options:**
1. **Document as a known limitation** (no code). Mitigations available today: replication
   (`-replicas`), or keeping a seed online until downloaders finish.
2. **Seed-side "super-seeding" mode (`seed -super`):**
   - The seed advertises only a few pieces at a time in its BITFIELD.
   - It reveals a new piece by HAVE once a previously revealed piece has been announced by
     another peer, so each piece leaves the seed about once before any piece is sent twice.
   - No protocol change.
   - Costs: slower start for the first downloaders, more seed-side state, and a new
     scenario-13 variant to prove it.
3. **A new REJECT reason** ("fetch elsewhere") for pieces the seed has already uploaded while
   others have never left it. A protocol minor-version change; requesters must handle it.

**Proposed:** option 2, implemented as an opt-in flag and measured in scenario 13 against the
default.
