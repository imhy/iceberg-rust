# Review: Split Planning Commit (caf5c03d0)

**Commit:** `caf5c03d0` — `feat(scan): add split planning for file scan tasks`
**Branch:** `split_planning` / `imhy/split_planning`
**Date:** Sat Apr 4, 2026
**Author:** Sokolov Sergey

---

## Overall Assessment

The implementation is **a solid, minimal first pass** at split planning that correctly mirrors Java's core splitting logic. The code is clean, well-documented, and has good unit test coverage for the pure splitting function. However, there are **several issues and missing pieces** compared to Iceberg Java.

---

## Issues Found

### 1. No Bin Packing / Task Grouping (Missing Feature) — PERFORMANCE GAP

**Java** has a two-stage process:
1. Split files → many small `FileScanTask`s
2. **Bin-pack** them into `CombinedScanTask` groups using `TableScanUtil.planTasks()` (sliding window bin packing with `lookback` and `openFileCost`)

The Rust implementation skips step 2 entirely. Each split becomes its own task in the stream. This means:
- If a 1 GB file is split into 8 × 128 MB row groups, the reader opens the same file 8 times independently
- Java would pack these into fewer `CombinedScanTask`s to reduce open-file overhead
- The design doc mentions this as a future optimization (Phase 2), but it's a **performance gap today**

**Impact:** For files with many small row groups, the overhead of opening the file multiple times (S3 HEAD requests, Parquet footer parsing) could **outweigh the parallelism benefits**.

### 2. `file_size_in_bytes` Field Is Misleading After Splitting — FOOTGUN

In `split_file_scan_task()`:
```rust
tasks.push(FileScanTask {
    start,
    length,
    record_count: None,
    ..task.clone()  // copies file_size_in_bytes = original full file size
});
```

Every split task has `file_size_in_bytes` = the **original full file size**, not the split's byte range. This is **intentionally correct** (it's used to skip stat requests per the doc comment), but:
- It's inconsistent: `task.length` ≠ `task.file_size_in_bytes` for split tasks
- Any downstream code that assumes `length == file_size_in_bytes` will break
- The `file_size_in_bytes` field name is misleading — it should perhaps be `original_file_size_in_bytes`

**This is not a bug**, but it's a footgun waiting to happen. A comment on the field would help.

### 3. `is_strictly_ascending` Check — Edge Case with Duplicates

```rust
fn is_strictly_ascending(offsets: &[i64]) -> bool {
    offsets.windows(2).all(|w| w[0] < w[1])
}
```

If `split_offsets = [100, 100, 500]` (duplicate offsets — shouldn't happen in valid Parquet but could from corrupted data), this returns `false` and falls through to FixedSize. That's **correct behavior** — Java does the same with `ArrayUtil.isStrictlyAscending()`. No issue here.

### 4. Negative Length Edge Case — POTENTIAL BUG

In OffsetsAware strategy:
```rust
let length = if i + 1 < offsets.len() {
    offsets[i + 1] as u64 - start
} else {
    file_size - start
};
```

If `offsets = [100, 500, 2000]` and `file_size = 1200`, the last task would compute:
```
length = 1200 - 2000 = underflow (i64→u64 via wrapping or panic in debug)
```

**This is a potential bug** if someone passes offsets that exceed file_size. In release mode, this silently produces a wrapped `u64` value, resulting in an incorrect `length`.

**Recommendation:** Add a guard:
```rust
if offsets.last().map(|&o| o as u64).unwrap_or(0) > file_size {
    return vec![task]; // or return an error
}
```

### 5. Missing Integration Tests — TEST GAP

The design doc lists these tests as planned:
- ✅ **Unit tests for `split_file_scan_task()`** — 7 tests, all present in `task.rs`
- ❌ **Integration test: `plan_files` produces split tasks** — **NOT implemented**
- ❌ **End-to-end: split tasks produce correct data** — **NOT implemented**
- ❌ **Backwards compatibility test** — **NOT implemented**
- ❌ **Files missing split_offsets** — **NOT implemented**

The only existing reader test (`test_file_splits_respect_byte_ranges`) tests the reader side, not the planning side. **There is no test that verifies `plan_files()` actually emits split tasks when `with_split_size()` is called.**

### 6. No Test for `with_split_size()` Builder Method

The builder method exists but no test calls `.with_split_size()` on a `TableScan` and verifies the output. This is a significant gap.

### 7. `record_count: None` — Correct, But Loses Information — DESIGN CHOICE

Java's `BaseContentScanTask.estimateRowsCount()` uses the fractional byte range to estimate row count per split:
```java
static long estimateRowsCount(long length, ContentFile<?> file) {
    long[] splitOffsets = splitOffsets(file);
    long splitOffset = splitOffsets != null ? splitOffsets[0] : 0L;
    double scannedFileFraction = ((double) length) / (file.fileSizeInBytes() - splitOffset);
    return (long) (scannedFileFraction * file.recordCount());
}
```

Rust sets `record_count = None` for all splits. This is **conservative and safe**, but Java does better by providing an estimate. The `record_count` field could be useful for downstream progress reporting.

---

## Comparison with Java

| Aspect | Java | Rust | Verdict |
|--------|------|------|---------|
| OffsetsAware splitting | ✅ `OffsetsAwareSplitScanTaskIterator` | ✅ inline loop | ✅ Matches |
| FixedSize splitting | ✅ `FixedSizeSplitScanTaskIterator` | ✅ while loop | ✅ Matches |
| Strictly ascending check | ✅ `ArrayUtil.isStrictlyAscending()` | ✅ `is_strictly_ascending()` | ✅ Matches |
| File format splittability check | ✅ `file.format().isSplittable()` | ❌ Not checked | ⚠️ Minor gap |
| Bin packing / task grouping | ✅ `BinPacking.PackingIterable` | ❌ Not implemented | ⚠️ Performance gap |
| Adaptive split size | ✅ `adjustSplitSize()` | ❌ Not implemented | ℹ️ Future enhancement |
| Row count estimation per split | ✅ fractional estimate | `None` | ℹ️ Conservative but safe |
| Task merging after split | ✅ `MergeableScanTask` | ❌ Not implemented | ℹ️ Future enhancement |
| `split_open_file_cost` parameter | ✅ Used in bin packing weight | ❌ Not implemented | ℹ️ Tied to bin packing |

### Key Java Files Referenced

| File | Purpose |
|------|---------|
| `BaseContentScanTask.java` | Core `split()` method — strategy selection |
| `TableScanUtil.java` | `splitFiles()`, `planTasks()`, bin packing orchestration |
| `OffsetsAwareSplitScanTaskIterator.java` | One split per row group/stripe offset |
| `FixedSizeSplitScanTaskIterator.java` | Fixed-size byte chunk splitting |
| `BinPacking.java` | Sliding-window bin packing algorithm |
| `BaseFileScanTask.SplitScanTask` | Split task with `canMerge`/`merge` support |
| `TableScanUtil.adjustSplitSize()` | Adaptive split size based on parallelism |

---

## Positive Observations

1. **Clean function design** — `split_file_scan_task()` is a pure function with no side effects, making it trivially testable
2. **Strategy selection matches Java** — OffsetsAware → FixedSize fallback is identical
3. **Good unit tests** — 7 unit tests cover basic cases, edge cases, and field preservation
4. **Backwards compatible** — `split_size: None` is the default, no behavior change
5. **`file_size_in_bytes` preservation** — correct for the "skip stat request" optimization
6. **Well-documented** — design doc is thorough and matches the implementation

---

## Recommendations (Priority Order)

| Priority | Action | Rationale |
|----------|--------|-----------|
| **HIGH** | Add integration test calling `.with_split_size()` on a real table and verifying `plan_files()` emits multiple tasks | No test currently exercises the planning path end-to-end |
| **HIGH** | Add guard against `offsets[i] > file_size` to prevent integer underflow | Release-mode silent corruption is a real risk |
| **MEDIUM** | Add end-to-end test reading data from split tasks and verifying correctness | Validates the reader handles split boundaries correctly |
| **MEDIUM** | Consider adding file format splittability check (skip Avro/non-splittable formats) | Java checks `file.format().isSplittable()` before splitting |
| **LOW** | Add doc comment on `file_size_in_bytes` clarifying it's the original file size, not split size | Prevents downstream misuse |
| **LOW** | Consider estimating `record_count` proportionally (like Java) instead of `None` | Useful for progress reporting |
| **FUTURE** | Implement bin packing (Phase 2 in design doc) | Reduces file-open overhead for multi-split files |

---

## Verdict

**The commit is correct as a minimal first implementation** but should not be merged without at least one integration test verifying the `plan_files()` → `with_split_size()` path works end-to-end. The core splitting logic in `task.rs` is well-tested and matches Java. The main gaps are missing integration tests and the absence of bin packing (which the design doc acknowledges as future work).

### Blocking Before Merge
1. Integration test for `with_split_size()` → `plan_files()` flow
2. Guard against `offset > file_size` underflow

### Acceptable As Future Work
- Bin packing / task grouping
- Adaptive split size
- Row count estimation per split
- Task merging support
