// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{
    DeleteStrategy, IterOptions, Iterable, Iterator, MiscExt, Mutable, Range, Result, WriteBatch,
    WriteBatchExt,
};
use tikv_util::keybuilder::KeyBuilder;

use crate::engine::AgateEngine;

impl MiscExt for AgateEngine {
    fn flush(&self, sync: bool) -> Result<()> {
        // TODO: Implement this for AgateDB.
        Ok(())
    }

    fn flush_cf(&self, cf: &str, sync: bool) -> Result<()> {
        // TODO: Implement this for AgateDB.
        Ok(())
    }

    fn delete_ranges_cf(
        &self,
        cf: &str,
        strategy: DeleteStrategy,
        ranges: &[Range<'_>],
    ) -> Result<()> {
        if ranges.is_empty() {
            return Ok(());
        }

        for range in ranges {
            let start = KeyBuilder::from_slice(range.start_key, 0, 0);
            let end = KeyBuilder::from_slice(range.end_key, 0, 0);
            let mut opts = IterOptions::new(Some(start), Some(end), false);
            let mut it = self.iterator_cf_opt(cf, opts)?;
            let mut it_valid = it.seek(range.start_key.into())?;
            let mut wb = self.write_batch();
            while it_valid {
                wb.delete_cf(cf, it.key())?;
                if wb.count() >= Self::WRITE_BATCH_MAX_KEYS {
                    wb.write()?;
                    wb.clear();
                }
                it_valid = it.next()?;
            }
            if wb.count() > 0 {
                wb.write()?;
            }
            self.sync_wal()?;
        }

        Ok(())
    }

    fn get_approximate_memtable_stats_cf(&self, cf: &str, range: &Range<'_>) -> Result<(u64, u64)> {
        // TODO: Maybe we could remove this method.
        Ok((0, 0))
    }

    fn ingest_maybe_slowdown_writes(&self, cf: &str) -> Result<bool> {
        // TODO: Implement this for AgateDB.
        Ok(false)
    }

    fn get_engine_used_size(&self) -> Result<u64> {
        // TODO: Implement this for AgateDB.
        // Not reasonable.
        Ok(0)
    }

    fn roughly_cleanup_ranges(&self, ranges: &[(Vec<u8>, Vec<u8>)]) -> Result<()> {
        // TODO: Only called in initializing RaftPollerBuilder, may could skip this now.
        Ok(())
    }

    fn path(&self) -> &str {
        self.path.to_str().unwrap()
    }

    fn sync_wal(&self) -> Result<()> {
        // TODO: Implement this for AgateDB.
        Ok(())
    }

    fn exists(path: &str) -> bool {
        // TODO: Implement this for AgateDB.
        true
    }

    fn dump_stats(&self) -> Result<String> {
        // TODO: Implement this for AgateDB.
        Ok("AgateEngine::dump_stats called".to_string())
    }

    fn get_latest_sequence_number(&self) -> u64 {
        // TODO: Check if this is correct.
        self.agate.get_next_ts()
    }

    fn get_oldest_snapshot_sequence_number(&self) -> Option<u64> {
        // TODO: Check if this is correct.
        Some(self.agate.get_read_until())
    }

    fn get_total_sst_files_size_cf(&self, cf: &str) -> Result<Option<u64>> {
        // TODO: Implement this for AgateDB.
        Ok(None)
    }

    fn get_range_entries_and_versions(
        &self,
        cf: &str,
        start: &[u8],
        end: &[u8],
    ) -> Result<Option<(u64, u64)>> {
        // TODO: Implement this for AgateDB.
        Ok(None)
    }

    fn is_stalled_or_stopped(&self) -> bool {
        // TODO: Implement this for AgateDB.
        false
    }
}
