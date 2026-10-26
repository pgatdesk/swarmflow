# 0007 — Phase 7 findings: request depth, and early seed departure

Date: 2026-10-26. Status: **decided by the owner.**
- **A:** keep depth 4.
- **B:** seed-side distribution in the style of Selective Repeat, as the owner proposed. It is
  the super-seeding idea with ARQ semantics.

## Outcome of B (implemented as `seed -distribute`)

How the Selective Repeat model maps onto seeding:

| Selective Repeat | seed distribution |
|---|---|
| frame | piece |
| send different frames in parallel | each peer is revealed a window of *distinct*, never-sent pieces |
| ACK | any peer announcing the piece (HAVE/BITFIELD) |
| sender window | revealed but unacknowledged pieces per peer (`-distribute-window`, default 8 ≥ request depth 4) |
| retransmit only lost frames | only pieces whose holder disconnected, or that went unacknowledged for 20 s, are re-assigned, ahead of untouched pieces |
| done | once every piece has been acknowledged, the seed advertises everything and seeds normally |

No protocol change was needed: peers only ever see a seed that appears to hold a moving handful
of pieces.

**Measured in-process** (`TestDistributeSurvivesEarlySeedDeparture`, 5 runs): with
`-distribute` the seed uploaded **1.00 copies**, left, and **4/4 downloaders completed**. With
normal seeding it uploaded **1.38 copies** and **0/4 completed**, with 2–8 pieces lost.

**Real-network results** (extra scenario 13, 5 trials per variant):
- Normal seeding: 0/40 downloaders completed.
- `-distribute` with tail stealing:
  - **40/40** when downloaders keep seeding 60 s after finishing;
  - 35/40 when they leave at once ("hit and run"). The 5 failures are the last straggler of each
    trial, whose sources left.
- Scenario 02, seed stays: no slowdown (30.8 s vs 31.4 s).

**Tail stealing** (added after the first lab run left 1 piece missing in 2 of 5 trials): when
nothing is unsent, revealed pieces that their holder has not requested within 200 ms move to an
idle peer.

**Side effect:** while a seed distributes, downloaders see the not-yet-revealed pieces as having
"no source" (`unavailable_pieces` > 0, `sources_lost` events). That is expected in this mode.

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
