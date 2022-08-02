// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::path::Path;

use engine_traits::{ImportExt, IngestExternalFileOptions, Result};

use crate::engine::AgateEngine;

impl ImportExt for AgateEngine {
    type IngestExternalFileOptions = AgateIngestExternalFileOptions;

    fn ingest_external_file_cf(&self, cf: &str, files: &[&str]) -> Result<()> {
        panic!()
    }
}

pub struct AgateIngestExternalFileOptions;

impl IngestExternalFileOptions for AgateIngestExternalFileOptions {
    fn new() -> Self {
        panic!()
    }

    fn move_files(&mut self, f: bool) {
        panic!()
    }

    fn get_write_global_seqno(&self) -> bool {
        panic!()
    }

    fn set_write_global_seqno(&mut self, f: bool) {
        panic!()
    }
}