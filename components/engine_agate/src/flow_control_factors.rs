// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{FlowControlFactorsExt, Result};

use crate::engine::AgateEngine;

impl FlowControlFactorsExt for AgateEngine {
    fn get_cf_num_files_at_level(&self, cf: &str, level: usize) -> Result<Option<u64>> {
        panic!()
    }

    fn get_cf_num_immutable_mem_table(&self, cf: &str) -> Result<Option<u64>> {
        panic!()
    }

    fn get_cf_pending_compaction_bytes(&self, cf: &str) -> Result<Option<u64>> {
        panic!()
    }
}
