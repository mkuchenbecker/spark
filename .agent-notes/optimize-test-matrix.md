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

## Phase 2 — column DDL  — GREEN (58/58); one real bug found + fixed
- [x] **C1** add non-key column — PASS (null backfill preserved, scope holds)
- [~] **C2** add col → add to `cluster.keys` — deferred to X2 (combined)
- [x] **C4** drop the leading-key column — PASS (loud failure, state untouched)
- [x] **C6** rename a non-key column — PASS (unaffected)
- [x] **C7** promote leading key int→BIGINT — **found + fixed a real bug** (see below), now PASS
- [x] **C8** reorder columns — PASS (incremental still correct)

## Phase 3 — partition-spec DDL  — GREEN
- [x] **P3** add a partition field — PASS
- [x] **P4** drop a partition field — PASS
- [x] **P6** incremental scope proof across a transform change — PASS (subsumes P5)
- [ ] **DECISION** cross-spec rewrite: does OPTIMIZE rewrite old-spec files into the new spec, or
  leave them? Still open — data is preserved either way; pin the intended physical behavior when
  Phase 3 is extended.

## Phase 4 — DML × incremental  — GREEN (64/64)
- [x] **D2** late data below watermark → incremental no-op, late files not rewritten — PASS
- [x] **D3** DELETE (CoW) → incremental keeps rows deleted — PASS
- [x] **D4** DELETE (MoR) in the incremental scope stays applied through the rewrite — PASS
- [x] **D5** UPDATE (MoR) → incremental stays consistent — PASS
- [x] **D6** MERGE (MoR) → incremental stays consistent — PASS
- [x] **D7** multi-round append→incremental — watermark monotonic, prior rounds not re-clustered — PASS

## Phase 5 — SE (rest) + OpenHouse  — GREEN (68/68)
- [x] **S1** SE prunes old snapshots → watermark property untouched, incremental runs — PASS
- [x] **S3** SE between incremental runs → clustering state + watermark property unchanged — PASS
- [ ] **S4** (OpenHouse itest) real OpenHouse SE service → state survives real SE — REMAINING (needs
  the OpenHouse embedded-server run; Hadoop-catalog `expire_snapshots` already covers S1–S3)

## Phase 6 — combined  — GREEN
- [~] **X1** spec change + append + incremental — subsumed by P6/P3
- [x] **X2** add-key-col + reconfigure + incremental (new epoch) — PASS
- [~] **X3** leading-key type promotion mid-history — subsumed by C7 (which found the valueGt bug)
- [x] **X4** SE (expires watermark) + spec change + incremental fallback — PASS

## Execution notes (append findings here)
- Phase 1 (2026-07-16): all 5 cells green, suite 50/50. Key result — **incremental is genuinely
  incremental** (D1 scope proof passes: already-clustered files are not rewritten). Rename of the
  leading key fails loudly rather than mis-scoping (C5). Partition-spec add + transform-change
  preserve data (P1/P2). Real snapshot-expiration of the watermark falls back cleanly (S2). No
  command bugs surfaced by Phase 1.
- Phase 2/3 (2026-07-16): 8 cells, suite 58/58 after a fix. **BUG FOUND + FIXED (C7):** promoting
  the leading-key type between runs (INT→BIGINT) crashed incremental OPTIMIZE with
  `ClassCastException: Integer cannot be cast to Long` in `OptimizeTableCommand.valueGt` — the
  watermark max (read as-of the old snapshot, boxed Integer) and the current max (Long) were
  compared via a raw `Comparable.compareTo`. Fixed `valueGt` to compare numerics by value
  (`BigDecimal(x.toString)`), falling back to natural ordering for date/timestamp/string. All other
  column-DDL (add/drop/rename/reorder) and partition-spec DDL (add/drop field, transform change)
  cells preserve data and hold incremental scope.

---

(Full matrix rationale mirrors scratchpad/optimize-dml-ddl-test-matrix.md: assertion bar A1–A5, the
per-cell risk table for column DDL, partition-spec DDL, DML, and SE. Concurrency/reconciliation is
out of scope.)
