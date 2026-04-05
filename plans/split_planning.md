# Split Planning for iceberg-rust

## Background

iceberg-rust has all the infrastructure for byte-range split reading, but lacks the planning layer that creates multiple `FileScanTask`s per file. Today, every file produces exactly one task with `start=0, length=file_size_in_bytes`.

### What already works

1. **split_offsets are written** — `ParquetWriter` (writer/file_writer/parquet_writer.rs:415-421) extracts row group offsets from Parquet metadata and stores them in `DataFile.split_offsets`.

2. **split_offsets API exists** — `DataFile::split_offsets()` (spec/manifest/data_file.rs:249-254) returns `Option<&[i64]>`, sorted ascending.

3. **Byte-range row group filtering works** — `ArrowReader::filter_row_groups_by_byte_range()` (arrow/reader.rs:1058-1082) selects row groups overlapping `[start, start+length)`. It's called when `task.start != 0 || task.length != 0` (arrow/reader.rs:476-485).

4. **FileScanTask has start/length fields** — (scan/task.rs:58-60), but always initialized to `start=0, length=file_size`.

### The gap

**scan/context.rs:119-122** — the only place FileScanTask is created:
```rust
Ok(FileScanTask {
    start: 0,
    length: self.manifest_entry.file_size_in_bytes(),
    record_count: Some(self.manifest_entry.record_count()),
    ...
})
```

`split_offsets` from `DataFile` are never consumed during scan planning.

### DataFusion impact

`IcebergTableScan` (integrations/datafusion/src/physical_plan/scan.rs:107-117) currently reports `Partitioning::UnknownPartitioning(1)` — one partition. DataFusion parallelism relies on multiple partitions. Split planning would allow reporting N partitions, enabling DataFusion to distribute work across threads.

## Plan

### Phase 1: Core split planning in iceberg crate

#### 1.1 Add split configuration to TableScanBuilder

**File**: `crates/iceberg/src/scan/mod.rs`

Add two fields to `TableScanBuilder`:
```rust
split_size: Option<u64>,            // target split size in bytes (None = no splitting)
split_open_file_cost: Option<u64>,  // estimated cost of opening a file (for bin-packing)
```

Add builder methods:
```rust
pub fn with_split_size(mut self, size: u64) -> Self { ... }
pub fn with_split_open_file_cost(mut self, cost: u64) -> Self { ... }
```

Pass through to `PlanContext` and `ManifestEntryContext` so split config is available when creating tasks.

Default: `None` (no splitting) for backwards compatibility. Java default is 128 MB.

#### 1.2 Add split_file_scan_task function

**File**: `crates/iceberg/src/scan/task.rs`

Add a method that takes a full-file `FileScanTask` and splits it using `split_offsets` from the `DataFile`:

```rust
/// Split a FileScanTask into multiple tasks using split offsets (e.g., row group boundaries).
/// Returns a Vec of tasks, each covering a byte range corresponding to one or more split offsets.
/// If no split offsets are available, returns the original task unchanged.
pub fn split_file_scan_task(
    task: FileScanTask,
    split_offsets: Option<&[i64]>,
    file_size: u64,
) -> Vec<FileScanTask>
```

Two strategies (matching Java's `BaseContentScanTask.split()`):

- **OffsetsAware** — when `split_offsets` are present and strictly ascending: create one task per offset. `start = offsets[i]`, `length = offsets[i+1] - offsets[i]` (last: `file_size - offsets[last]`).

- **FixedSize** — when split_offsets are absent: divide file into chunks of `target_split_size`. `start = i * target_split_size`, `length = min(target_split_size, remaining)`.

For split tasks, set `record_count = None` (can't know row count per split without reading metadata).

#### 1.3 Integrate into scan planning

**File**: `crates/iceberg/src/scan/context.rs`

In `ManifestEntryContext::into_file_scan_task()`, instead of creating a single task:

1. Access `self.manifest_entry.data_file().split_offsets()`
2. If split planning is enabled (split_size is Some in config):
   - Call `split_file_scan_task()` with the offsets and file size
   - Return a `Vec<FileScanTask>` instead of a single task

**File**: `crates/iceberg/src/scan/mod.rs`

In `plan_files()`, change the stream to flatten split results. The signature `FileScanTaskStream` (a `BoxStream<Result<FileScanTask>>`) stays the same — each split task is emitted as a separate stream element.

### Phase 2: DataFusion integration

#### 2.1 Expose split tasks as DataFusion partitions

**File**: `crates/integrations/datafusion/src/physical_plan/scan.rs`

Currently `IcebergTableScan` reports 1 partition. With split planning, it could:

1. Collect all FileScanTasks upfront (during planning, not execution)
2. Group them into N partitions (round-robin or bin-packing)
3. Report `Partitioning::RoundRobin(N)` in `compute_properties()`
4. In `execute(partition)`, return only that partition's tasks

This is a larger change and can be done separately.

### Key files

| File | Change |
|------|--------|
| `crates/iceberg/src/scan/mod.rs` | Add split_size config to TableScanBuilder, PlanContext |
| `crates/iceberg/src/scan/task.rs` | Add `split_file_scan_task()` function |
| `crates/iceberg/src/scan/context.rs` | Use split_offsets in into_file_scan_task() |
| `crates/iceberg/src/arrow/reader.rs` | No changes needed — byte-range filtering already works |

### Tests

#### Unit tests for `split_file_scan_task()` (task.rs)

Pure logic tests — no I/O needed. Construct a `FileScanTask` and verify the output.

| Test | Input | Expected |
|------|-------|----------|
| **offsets_aware_basic** | `split_offsets = [100, 500, 900]`, `file_size = 1200` | 3 tasks: `(100, 400)`, `(500, 400)`, `(900, 300)` |
| **fixed_size_basic** | no split_offsets, `file_size = 1000`, `target = 300` | 4 tasks: `(0, 300)`, `(300, 300)`, `(600, 300)`, `(900, 100)` |
| **single_offset** | `split_offsets = [100]`, `file_size = 500` | 1 task: `(100, 400)` |
| **no_splitting_disabled** | `split_offsets = None`, no target size | Returns original task unchanged (`start=0, length=file_size`) |
| **empty_offsets_vec** | `split_offsets = Some(vec![])` | Returns original task unchanged |
| **record_count_cleared** | Any split input | All split tasks have `record_count = None` |
| **fields_preserved** | Any split input | `data_file_path`, `schema`, `predicate`, `deletes`, etc. cloned correctly to each split task |

#### Integration test: plan_files produces split tasks (scan/mod.rs)

Tests the planning pipeline end-to-end without reading data.

1. Write a multi-row-group Parquet file using iceberg-rust's `ParquetWriter` (e.g., 1000 rows, `max_row_group_size = 200` → 5 row groups)
2. Create a table, append the data file (with `split_offsets` populated by the writer)
3. Call `table.scan().with_split_size(small_value).plan_files().await`
4. Collect all `FileScanTask`s from the stream
5. Assert:
   - More than 1 task produced for the single file
   - All tasks reference the same `data_file_path`
   - Each task has different `start`/`length`
   - Tasks cover the full file: first task's `start` = smallest split offset, last task's `start + length` = file_size
   - No overlap between consecutive tasks
   - `record_count = None` for all split tasks
   - `file_size_in_bytes` is the same across all tasks

#### End-to-end: split tasks produce correct data (arrow/reader.rs)

Exercises the existing `filter_row_groups_by_byte_range()` with real split boundaries.

1. Same multi-row-group file as above
2. Scan with `with_split_size()` enabled
3. Read via `to_arrow()` → collect all `RecordBatch`es
4. Concatenate and verify:
   - Total row count matches expected (e.g., 1000)
   - No duplicate rows
   - No missing rows
   - Values are correct (e.g., sequential ids 0..999)
5. Compare with a non-split scan of the same file — results should be identical

#### Backwards compatibility

1. Same multi-row-group file
2. Scan **without** `with_split_size()` (default = None)
3. Collect tasks from `plan_files()`
4. Assert: exactly 1 `FileScanTask` per file, `start = 0`, `length = file_size`, `record_count = Some(...)`

#### Files missing split_offsets (migrated tables)

1. Create a `DataFile` entry without `split_offsets` (simulating a migrated/imported table)
2. Scan with `with_split_size(target)` enabled
3. Assert: falls back to FixedSize splitting — multiple tasks with `start = i * target`, `length = min(target, remaining)`
4. Read all tasks and verify complete dataset

#### Existing reader test (already passes)

`test_file_splits_respect_byte_ranges` (arrow/reader.rs:2506-2697) already tests the reader side:
- Creates 3 row groups with known byte positions
- Reads with two tasks: one for RG 0, one for RG 1+2
- Verifies correct data ranges returned

This test validates that once split planning produces correct `start`/`length`, the reader handles them correctly. No changes needed.

### Verification

```bash
cd /home/svs/workspaces/iceberg_ws/iceberg-rust
cargo test -p iceberg --lib
cargo test -p iceberg --test '*'   # integration tests if applicable
```

### Notes

- The existing `filter_row_groups_by_byte_range()` uses cumulative `compressed_size()` to compute row group positions. For files written by iceberg-rust's ParquetWriter, `split_offsets` come from `file_offset()` — these may not match exactly. However, split planning uses the offsets to define task boundaries, and the reader uses its own row group detection. As long as each row group falls within exactly one task's range, correctness is preserved.

- Java's split planning also includes bin-packing (`TableScanUtil.planTaskGroups()`) which groups small splits into task groups. This is a further optimization that could be added later.
