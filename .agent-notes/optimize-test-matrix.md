# OPTIMIZE incremental — DML/DDL test matrix: execution checklist

Persistent plan + progress for hardening `OptimizeClusteringIcebergSuite` against DML mutation and
DDL evolution (partition spec + columns) between incremental runs, plus real snapshot-expiration (SE).
Full rationale/matrix at the bottom; this top section is the live checklist.

## Assertion bar (foundation — do first)
The current `assertPreserves` cannot distinguish incremental from full (both preserve data + commit +
advance the watermark). Add the scope-proof primitive:
- [x] **F1** `assertIncrementalScope` helper — DONE. Uses `dataFiles`; asserts run-1 files ⊆
  post-run-2 files (old clustered files not rewritten) AND appended files were rewritten.

## Phase 1 — highest risk / zero current coverage  — ALL GREEN (50/50 suite)
- [x] **D1** incremental rewrites only the new slice — PASS. Proves the command genuinely scopes
  (old clustered files survive untouched). The previously-missing core assertion now exists + passes.
- [x] **C5** rename leading-key column — PASS. OPTIMIZE **fails loudly** (rewrite aborts →
  exception); state + watermark left untouched. No silent mis-scope. Good behavior, now pinned.
- [x] **P1** add `days(ts)` partition field → incremental — PASS. Data preserved across spec add.
- [x] **P2** `days(ts)`→`hours(ts)` transform change → incremental — PASS. Data preserved.
- [x] **S2** real `expire_snapshots` of the watermark → fallback to full backfill — PASS (real
  expiry, not the fake id). Watermark reset off the expired snapshot.

## Phase 2 — column DDL
- [ ] **C1** add non-key column (null backfill preserved, scope unaffected)
- [ ] **C2** add col → add to `cluster.keys` (new epoch, old retained)
- [ ] **C4** drop the leading-key column → loud failure, state untouched
- [ ] **C6** rename/drop a non-key column → unaffected
- [ ] **C7** type-promote leading key (int→long, decimal precision) → scope + coverage/depth follow type
- [ ] **C8** reorder columns → no-op (nothing rewritten)

## Phase 3 — partition-spec DDL
- [ ] **P3** add a partition field
- [ ] **P4** drop a partition field
- [ ] **P5** spec field == leading key, then evolve transform
- [ ] **P6** spec change → append → incremental (scope picks right files under new spec)
- [ ] **DECISION** cross-spec rewrite: does OPTIMIZE rewrite old-spec files into the new spec, or
  leave them? Pin the intended behavior, assert everywhere.

## Phase 4 — DML × incremental
- [ ] **D2** late data below watermark → not re-touched; coverage over-reports, depth exposes it
- [ ] **D3** DELETE (CoW) → incremental
- [ ] **D4** DELETE (MoR) → incremental (delete files preserved)
- [ ] **D5** UPDATE (MoR) → incremental
- [ ] **D6** MERGE (MoR) → incremental
- [ ] **D7** multi-round append→incremental×N (watermark monotonic, no re-cluster of prior rounds)

## Phase 5 — SE (rest) + OpenHouse
- [ ] **S1** SE prunes snapshots older than watermark → incremental robust
- [ ] **S3** SE between two incremental runs → coverage/state unchanged
- [ ] **S4** (OpenHouse itest) real OpenHouse SE service → state survives real SE

## Phase 6 — combined (after 1–5 green)
- [ ] **X1** spec change + append + incremental
- [ ] **X2** add-key-col + reconfigure + incremental
- [ ] **X3** leading-key type promotion mid-history
- [ ] **X4** SE + spec change + incremental

## Execution notes (append findings here)
- Phase 1 (2026-07-16): all 5 cells green, suite 50/50. Key result — **incremental is genuinely
  incremental** (D1 scope proof passes: already-clustered files are not rewritten). Rename of the
  leading key fails loudly rather than mis-scoping (C5). Partition-spec add + transform-change
  preserve data (P1/P2). Real snapshot-expiration of the watermark falls back cleanly (S2). No
  command bugs surfaced by Phase 1.

---

(Full matrix rationale mirrors scratchpad/optimize-dml-ddl-test-matrix.md: assertion bar A1–A5, the
per-cell risk table for column DDL, partition-spec DDL, DML, and SE. Concurrency/reconciliation is
out of scope.)
