// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::BTreeMap;

use engine_traits::{CompactExt, CompactedEvent, Result};

use crate::engine::AgateEngine;

// TODO: Implement these for AgateDB.
impl CompactExt for AgateEngine {
    type CompactedEvent = AgateCompactedEvent;

    fn auto_compactions_is_disabled(&self) -> Result<bool> {
        Ok(false)
    }

    fn compact_range(
        &self,
        cf: &str,
        start_key: Option<&[u8]>,
        end_key: Option<&[u8]>,
        exclusive_manual: bool,
        max_subcompactions: u32,
    ) -> Result<()> {
        Ok(())
    }

    fn compact_files_in_range(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        output_level: Option<i32>,
    ) -> Result<()> {
        Ok(())
    }

    fn compact_files_in_range_cf(
        &self,
        cf: &str,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        output_level: Option<i32>,
    ) -> Result<()> {
        Ok(())
    }

    fn compact_files_cf(
        &self,
        cf: &str,
        files: Vec<String>,
        output_level: Option<i32>,
        max_subcompactions: u32,
        exclude_l0: bool,
    ) -> Result<()> {
        Ok(())
    }
}

pub struct AgateCompactedEvent;

// TODO: Make size declining not trivial AgateDB.
impl CompactedEvent for AgateCompactedEvent {
    fn total_bytes_declined(&self) -> u64 {
        0
    }

    fn is_size_declining_trivial(&self, split_check_diff: u64) -> bool {
        true
    }

    fn output_level_label(&self) -> String {
        "AgateCompactedEvent::CompactedEvent".to_string()
    }

    fn calc_ranges_declined_bytes(
        self,
        ranges: &BTreeMap<Vec<u8>, u64>,
        bytes_threshold: u64,
    ) -> Vec<(u64, u64)> {
        vec![]
    }

    fn cf(&self) -> &str {
        "AgateCompactedEvent"
    }
}
