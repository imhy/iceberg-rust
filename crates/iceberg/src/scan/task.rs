// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::sync::Arc;

use futures::stream::BoxStream;
use serde::{Deserialize, Serialize, Serializer};

use crate::Result;
use crate::expr::BoundPredicate;
use crate::spec::{
    DataContentType, DataFileFormat, ManifestEntryRef, NameMapping, PartitionSpec, Schema,
    SchemaRef, Struct,
};

/// A stream of [`FileScanTask`].
pub type FileScanTaskStream = BoxStream<'static, Result<FileScanTask>>;

/// Serialization helper that always returns NotImplementedError.
/// Used for fields that should not be serialized but we want to be explicit about it.
fn serialize_not_implemented<S, T>(_: &T, _: S) -> std::result::Result<S::Ok, S::Error>
where S: Serializer {
    Err(serde::ser::Error::custom(
        "Serialization not implemented for this field",
    ))
}

/// Deserialization helper that always returns NotImplementedError.
/// Used for fields that should not be deserialized but we want to be explicit about it.
fn deserialize_not_implemented<'de, D, T>(_: D) -> std::result::Result<T, D::Error>
where D: serde::Deserializer<'de> {
    Err(serde::de::Error::custom(
        "Deserialization not implemented for this field",
    ))
}

/// A task to scan part of file.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FileScanTask {
    /// The total size of the data file in bytes, from the manifest entry.
    /// Used to skip a stat/HEAD request when reading Parquet footers.
    pub file_size_in_bytes: u64,
    /// The start offset of the file to scan.
    pub start: u64,
    /// The length of the file to scan.
    pub length: u64,
    /// The number of records in the file to scan.
    ///
    /// This is an optional field, and only available if we are
    /// reading the entire data file.
    pub record_count: Option<u64>,

    /// The data file path corresponding to the task.
    pub data_file_path: String,

    /// The format of the file to scan.
    pub data_file_format: DataFileFormat,

    /// The schema of the file to scan.
    pub schema: SchemaRef,
    /// The field ids to project.
    pub project_field_ids: Vec<i32>,
    /// The predicate to filter.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub predicate: Option<BoundPredicate>,

    /// The list of delete files that may need to be applied to this data file
    pub deletes: Vec<FileScanTaskDeleteFile>,

    /// Partition data from the manifest entry, used to identify which columns can use
    /// constant values from partition metadata vs. reading from the data file.
    /// Per the Iceberg spec, only identity-transformed partition fields should use constants.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(serialize_with = "serialize_not_implemented")]
    #[serde(deserialize_with = "deserialize_not_implemented")]
    pub partition: Option<Struct>,

    /// The partition spec for this file, used to distinguish identity transforms
    /// (which use partition metadata constants) from non-identity transforms like
    /// bucket/truncate (which must read source columns from the data file).
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(serialize_with = "serialize_not_implemented")]
    #[serde(deserialize_with = "deserialize_not_implemented")]
    pub partition_spec: Option<Arc<PartitionSpec>>,

    /// Name mapping from table metadata (property: schema.name-mapping.default),
    /// used to resolve field IDs from column names when Parquet files lack field IDs
    /// or have field ID conflicts.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(serialize_with = "serialize_not_implemented")]
    #[serde(deserialize_with = "deserialize_not_implemented")]
    pub name_mapping: Option<Arc<NameMapping>>,

    /// Whether this scan task should treat column names as case-sensitive when binding predicates.
    pub case_sensitive: bool,
}

/// Splits a [`FileScanTask`] into multiple tasks based on split offsets (e.g., Parquet row group
/// boundaries) or a target split size.
///
/// Two strategies (matching Java's `BaseContentScanTask.split()`):
/// - **OffsetsAware**: when `split_offsets` are present and strictly ascending, creates one task
///   per offset boundary. Each task covers `[offsets[i], offsets[i+1])`, with the last task
///   extending to `file_size`.
/// - **FixedSize**: when `split_offsets` are absent, divides the file into chunks of
///   `target_split_size`.
///
/// Split tasks have `record_count = None` since the per-split row count is unknown without
/// reading Parquet metadata.
pub fn split_file_scan_task(
    task: FileScanTask,
    split_offsets: Option<&[i64]>,
    target_split_size: u64,
    file_size: u64,
) -> Vec<FileScanTask> {

    // Try OffsetsAware strategy: use split_offsets if present and valid
    if let Some(offsets) = split_offsets
        && !offsets.is_empty()
        && is_strictly_ascending(offsets)
    {
        let mut tasks = Vec::with_capacity(offsets.len());
        for i in 0..offsets.len() {
            let start = offsets[i] as u64;
            let length = if i + 1 < offsets.len() {
                offsets[i + 1] as u64 - start
            } else {
                file_size - start
            };

            tasks.push(FileScanTask {
                start,
                length,
                record_count: None,
                ..task.clone()
            });
        }
        return tasks;
    }

    // FixedSize strategy: divide file into chunks of target_split_size
    if target_split_size > 0 && file_size > target_split_size {
        let mut tasks = Vec::new();
        let mut offset = 0u64;
        while offset < file_size {
            let length = std::cmp::min(target_split_size, file_size - offset);
            tasks.push(FileScanTask {
                start: offset,
                length,
                record_count: None,
                ..task.clone()
            });
            offset += length;
        }
        return tasks;
    }

    // No splitting: return original task as-is
    vec![task]
}

/// Returns true if the slice is strictly ascending (each element > previous).
fn is_strictly_ascending(offsets: &[i64]) -> bool {
    offsets.windows(2).all(|w| w[0] < w[1])
}

impl FileScanTask {
    /// Returns the data file path of this file scan task.
    pub fn data_file_path(&self) -> &str {
        &self.data_file_path
    }

    /// Returns the project field id of this file scan task.
    pub fn project_field_ids(&self) -> &[i32] {
        &self.project_field_ids
    }

    /// Returns the predicate of this file scan task.
    pub fn predicate(&self) -> Option<&BoundPredicate> {
        self.predicate.as_ref()
    }

    /// Returns the schema of this file scan task as a reference
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Returns the schema of this file scan task as a SchemaRef
    pub fn schema_ref(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[derive(Debug)]
pub(crate) struct DeleteFileContext {
    pub(crate) manifest_entry: ManifestEntryRef,
    pub(crate) partition_spec_id: i32,
}

impl From<&DeleteFileContext> for FileScanTaskDeleteFile {
    fn from(ctx: &DeleteFileContext) -> Self {
        FileScanTaskDeleteFile {
            file_path: ctx.manifest_entry.file_path().to_string(),
            file_size_in_bytes: ctx.manifest_entry.file_size_in_bytes(),
            file_type: ctx.manifest_entry.content_type(),
            partition_spec_id: ctx.partition_spec_id,
            equality_ids: ctx.manifest_entry.data_file.equality_ids.clone(),
        }
    }
}

/// A task to scan part of file.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FileScanTaskDeleteFile {
    /// The delete file path
    pub file_path: String,

    /// The total size of the delete file in bytes, from the manifest entry.
    pub file_size_in_bytes: u64,

    /// delete file type
    pub file_type: DataContentType,

    /// partition id
    pub partition_spec_id: i32,

    /// equality ids for equality deletes (null for anything other than equality-deletes)
    pub equality_ids: Option<Vec<i32>>,
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::spec::{DataFileFormat, Schema, SchemaRef};

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::builder().build().unwrap())
    }

    fn make_task(file_size: u64) -> FileScanTask {
        FileScanTask {
            file_size_in_bytes: file_size,
            start: 0,
            length: file_size,
            record_count: Some(1000),
            data_file_path: "s3://bucket/data/file.parquet".to_string(),
            data_file_format: DataFileFormat::Parquet,
            schema: test_schema(),
            project_field_ids: vec![1, 2, 3],
            predicate: None,
            deletes: vec![],
            partition: None,
            partition_spec: None,
            name_mapping: None,
            case_sensitive: true,
        }
    }

    #[test]
    fn test_offsets_aware_basic() {
        let task = make_task(1200);
        let offsets = vec![100i64, 500, 900];
        let tasks = split_file_scan_task(task, Some(&offsets), 128 * 1024 * 1024, 1200);

        assert_eq!(tasks.len(), 3);
        assert_eq!((tasks[0].start, tasks[0].length), (100, 400));
        assert_eq!((tasks[1].start, tasks[1].length), (500, 400));
        assert_eq!((tasks[2].start, tasks[2].length), (900, 300));
    }

    #[test]
    fn test_fixed_size_basic() {
        let task = make_task(1000);
        let tasks = split_file_scan_task(task, None, 300, 1000);

        assert_eq!(tasks.len(), 4);
        assert_eq!((tasks[0].start, tasks[0].length), (0, 300));
        assert_eq!((tasks[1].start, tasks[1].length), (300, 300));
        assert_eq!((tasks[2].start, tasks[2].length), (600, 300));
        assert_eq!((tasks[3].start, tasks[3].length), (900, 100));
    }

    #[test]
    fn test_single_offset() {
        let task = make_task(500);
        let offsets = vec![100i64];
        let tasks = split_file_scan_task(task, Some(&offsets), 128 * 1024 * 1024, 500);

        assert_eq!(tasks.len(), 1);
        assert_eq!((tasks[0].start, tasks[0].length), (100, 400));
    }

    #[test]
    fn test_no_splitting_disabled() {
        let task = make_task(500);
        // No split_offsets and target larger than file → no splitting
        let tasks = split_file_scan_task(task.clone(), None, 128 * 1024 * 1024, 500);

        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].start, 0);
        assert_eq!(tasks[0].length, 500);
        assert_eq!(tasks[0].record_count, Some(1000)); // preserved
    }

    #[test]
    fn test_empty_offsets_vec() {
        let task = make_task(500);
        let offsets: Vec<i64> = vec![];
        // Empty offsets, target larger than file → no splitting
        let tasks = split_file_scan_task(task, Some(&offsets), 128 * 1024 * 1024, 500);

        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].start, 0);
        assert_eq!(tasks[0].length, 500);
    }

    #[test]
    fn test_record_count_cleared() {
        let task = make_task(1200);
        assert_eq!(task.record_count, Some(1000));

        let offsets = vec![100i64, 500, 900];
        let tasks = split_file_scan_task(task, Some(&offsets), 128 * 1024 * 1024, 1200);

        for t in &tasks {
            assert_eq!(t.record_count, None, "split tasks should have record_count = None");
        }
    }

    #[test]
    fn test_fields_preserved() {
        let task = make_task(1200);
        let offsets = vec![100i64, 500];
        let tasks = split_file_scan_task(task, Some(&offsets), 128 * 1024 * 1024, 1200);

        assert_eq!(tasks.len(), 2);
        for t in &tasks {
            assert_eq!(t.data_file_path, "s3://bucket/data/file.parquet");
            assert_eq!(t.data_file_format, DataFileFormat::Parquet);
            assert_eq!(t.project_field_ids, vec![1, 2, 3]);
            assert_eq!(t.file_size_in_bytes, 1200);
            assert!(t.case_sensitive);
        }
    }
}
