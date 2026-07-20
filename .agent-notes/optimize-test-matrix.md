# OPTIMIZE incremental — DML/DDL test matrix: execution checklist

Persistent plan + progress for hardening `OptimizeClusteringIcebergSuite` against DML mutation and
DDL evolution (partition spec + columns) between incremental runs, plus real snapshot-expiration (SE).
Full rationale/matrix at the bottom; this top section is the live checklist.

## Assertion bar (foundation — do first)
The current `assertPreserves` cannot distinguish incremental from full (both preserve data + commit +
advance the watermark). Add the scope-proof primitive:
- [ ] **F1** `assertIncrementalScope` helper: capture `dataFiles` after run 1; append forward-slice
  data; run incremental; assert the run-1 file set is a **subset** of the post-run-2 file set (old
  clustered files NOT rewritten) AND the appended files WERE rewritten (removed). Uses existing
  `dataFiles(t): Set[String]`.

## Phase 1 — highest risk / zero current coverage
- [ ] **D1** forward-slice append → incremental rewrites only the new slice (F1 assertion). THE core.
- [ ] **C5** rename the leading-key column between runs → must not silently mis-scope. Probe actual
  behavior first; assert loud failure or correct handling (never silent wrong scope).
- [ ] **P1** unpartitioned → `ALTER TABLE ADD PARTITION FIELD days(ts)` → append → incremental.
- [ ] **P2** `days(ts)` → `hours(ts)` transform change → append → incremental.
- [ ] **S2** real SE: `expire_snapshots` expires the watermark snapshot → fallback to full backfill
  (replaces the current fake-`999999`-id test with a real expiry).

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
- (findings/bugs surfaced by the tests go here as they happen)

---

(Full matrix rationale mirrors scratchpad/optimize-dml-ddl-test-matrix.md: assertion bar A1–A5, the
per-cell risk table for column DDL, partition-spec DDL, DML, and SE. Concurrency/reconciliation is
out of scope.)
