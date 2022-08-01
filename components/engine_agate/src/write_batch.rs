// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{Arc, Mutex};

use agatedb::{Agate, IteratorOptions};
use bytes::Bytes;
use engine_traits::{Mutable, Result, WriteBatch, WriteBatchExt, WriteOptions, CF_DEFAULT};

use crate::{
    engine::AgateEngine,
    utils::{add_cf_prefix, get_cf_and_key},
};

#[derive(Clone)]
enum WriteBatchOpType {
    Put(Bytes, Bytes, Option<String>),
    Delete(Bytes, Option<String>),
    DeleteRange(Bytes, Bytes, Option<String>),
}

impl WriteBatchExt for AgateEngine {
    type WriteBatch = AgateWriteBatch;

    // TODO: Maybe too large?
    const WRITE_BATCH_MAX_KEYS: usize = 128;

    fn write_batch(&self) -> Self::WriteBatch {
        AgateWriteBatch::new(self.agate.clone())
    }

    fn write_batch_with_cap(&self, cap: usize) -> Self::WriteBatch {
        // TODO: Consider capacity.
        self.write_batch()
    }
}

struct AgateWriteBatchInner {
    operations: Vec<WriteBatchOpType>,
    save_points: Vec<usize>,
}

pub struct AgateWriteBatch {
    agate: Arc<Agate>,
    inner: Mutex<AgateWriteBatchInner>,
}

impl AgateWriteBatch {
    pub fn new(agate: Arc<Agate>) -> AgateWriteBatch {
        AgateWriteBatch {
            agate,
            inner: Mutex::new(AgateWriteBatchInner {
                operations: vec![],
                save_points: vec![],
            }),
        }
    }

    pub fn get_db(&self) -> Arc<Agate> {
        self.agate.clone()
    }
}

impl WriteBatch for AgateWriteBatch {
    fn write_opt(&self, _: &WriteOptions) -> Result<()> {
        let mut txn = self.agate.new_transaction(true);

        let mut wb = self.inner.lock().unwrap();

        // TODO: Check if cf exists.
        // TODO: Avoid unnecessary clone.

        for op in &wb.operations {
            match op {
                WriteBatchOpType::Put(key, value, cf) => {
                    let key = add_cf_prefix(key, cf.clone());
                    txn.set(Bytes::from(key), value.clone())
                        .map_err(|e| engine_traits::Error::Engine(e.to_string()))?;
                }
                WriteBatchOpType::Delete(key, cf) => {
                    let key = add_cf_prefix(key, cf.clone());
                    txn.delete(Bytes::from(key))
                        .map_err(|e| engine_traits::Error::Engine(e.to_string()))?;
                }
                WriteBatchOpType::DeleteRange(begin_key, end_key, cf) => {
                    if end_key < begin_key {
                        return Err(engine_traits::Error::Engine(
                            "end_key should be equal or greater than begin_key".to_string(),
                        ));
                    }

                    let begin_key = add_cf_prefix(begin_key, cf.clone());
                    let end_key = add_cf_prefix(end_key, cf.clone());

                    let mut iter = txn.new_iterator(&IteratorOptions::default());
                    iter.seek(&Bytes::from(begin_key.clone()));

                    let is_valid = |iter: &agatedb::Iterator| {
                        if !iter.valid() {
                            return false;
                        }

                        let (cf_name, _) = get_cf_and_key(iter.item().key());

                        let cf_name_match = match cf {
                            Some(cf) => cf_name == *cf,
                            None => cf_name == CF_DEFAULT,
                        };

                        if !cf_name_match {
                            return false;
                        }

                        if !begin_key.is_empty() && iter.item().key() < &begin_key[..] {
                            return false;
                        }
                        if !end_key.is_empty() && iter.item().key() >= &end_key[..] {
                            return false;
                        }

                        true
                    };

                    while is_valid(&iter) {
                        txn.delete(Bytes::copy_from_slice(iter.item().key()))
                            .map_err(|e| engine_traits::Error::Engine(e.to_string()))?;
                        iter.next();
                    }
                }
            }
        }

        txn.commit()
            .map_err(|e| engine_traits::Error::Engine(e.to_string()))
    }

    fn data_size(&self) -> usize {
        // TODO: May not precise.

        let mut wb = self.inner.lock().unwrap();

        wb.operations
            .iter()
            .map(|op| match op {
                WriteBatchOpType::Put(key, value, cf) => {
                    key.len() + value.len() + cf.as_ref().map(|cf| cf.len()).unwrap_or(0)
                }
                WriteBatchOpType::Delete(key, cf) => {
                    key.len() + cf.as_ref().map(|cf| cf.len()).unwrap_or(0)
                }
                WriteBatchOpType::DeleteRange(begin_key, end_key, cf) => {
                    begin_key.len() + end_key.len() + cf.as_ref().map(|cf| cf.len()).unwrap_or(0)
                }
            })
            .sum()
    }

    fn count(&self) -> usize {
        let mut wb = self.inner.lock().unwrap();
        wb.operations.len()
    }

    fn is_empty(&self) -> bool {
        let mut wb = self.inner.lock().unwrap();
        wb.operations.is_empty()
    }

    fn should_write_to_engine(&self) -> bool {
        self.count() > AgateEngine::WRITE_BATCH_MAX_KEYS
    }

    fn clear(&mut self) {
        let mut wb = self.inner.lock().unwrap();

        wb.operations.clear();
        wb.save_points.clear();
    }

    fn set_save_point(&mut self) {
        let mut wb = self.inner.lock().unwrap();
        let save_point = wb.operations.len();
        wb.save_points.push(save_point);
    }

    fn pop_save_point(&mut self) -> Result<()> {
        let mut wb = self.inner.lock().unwrap();

        if wb.save_points.is_empty() {
            Err(engine_traits::Error::Engine("No save point".to_string()))
        } else {
            wb.save_points.pop();
            Ok(())
        }
    }

    fn rollback_to_save_point(&mut self) -> Result<()> {
        let mut wb = self.inner.lock().unwrap();

        if wb.save_points.is_empty() {
            Err(engine_traits::Error::Engine("No save point".to_string()))
        } else {
            let save_point = wb.save_points.pop().unwrap();
            wb.operations.truncate(save_point);
            Ok(())
        }
    }

    fn merge(&mut self, src: Self) -> Result<()> {
        let mut wb = self.inner.lock().unwrap();
        let src_wb = src.inner.lock().unwrap();

        wb.operations.extend(src_wb.operations.clone());
        Ok(())
    }
}

impl Mutable for AgateWriteBatch {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut wb = self.inner.lock().unwrap();

        wb.operations.push(WriteBatchOpType::Put(
            Bytes::copy_from_slice(key),
            Bytes::copy_from_slice(value),
            None,
        ));

        Ok(())
    }
    fn put_cf(&mut self, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        let mut wb = self.inner.lock().unwrap();

        wb.operations.push(WriteBatchOpType::Put(
            Bytes::copy_from_slice(key),
            Bytes::copy_from_slice(value),
            Some(cf.to_string()),
        ));

        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        let mut wb = self.inner.lock().unwrap();

        wb.operations
            .push(WriteBatchOpType::Delete(Bytes::copy_from_slice(key), None));

        Ok(())
    }
    fn delete_cf(&mut self, cf: &str, key: &[u8]) -> Result<()> {
        let mut wb = self.inner.lock().unwrap();

        wb.operations.push(WriteBatchOpType::Delete(
            Bytes::copy_from_slice(key),
            Some(cf.to_string()),
        ));

        Ok(())
    }
    fn delete_range(&mut self, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        let mut wb = self.inner.lock().unwrap();

        wb.operations.push(WriteBatchOpType::DeleteRange(
            Bytes::copy_from_slice(begin_key),
            Bytes::copy_from_slice(end_key),
            None,
        ));

        Ok(())
    }
    fn delete_range_cf(&mut self, cf: &str, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        let mut wb = self.inner.lock().unwrap();

        wb.operations.push(WriteBatchOpType::DeleteRange(
            Bytes::copy_from_slice(begin_key),
            Bytes::copy_from_slice(end_key),
            Some(cf.to_string()),
        ));

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use engine_traits::{Peekable, WriteBatch};
    use tempfile::Builder;

    use super::*;

    #[test]
    fn test_should_write_to_engine() {
        let path = Builder::new()
            .prefix("test-should-write-to-engine")
            .tempdir()
            .unwrap();

        let engine = AgateEngine::new(path.path(), vec![]);
        let mut wb = engine.write_batch();

        for _i in 0..AgateEngine::WRITE_BATCH_MAX_KEYS {
            wb.put(b"aaa", b"bbb").unwrap();
        }
        assert!(!wb.should_write_to_engine());

        wb.put(b"aaa", b"bbb").unwrap();
        assert!(wb.should_write_to_engine());

        wb.write().unwrap();

        let v = engine.get_value(b"aaa").unwrap();
        assert!(v.is_some());
        assert_eq!(v.unwrap(), b"bbb");

        let mut wb = engine.write_batch();
        for _i in 0..AgateEngine::WRITE_BATCH_MAX_KEYS {
            wb.put(b"aaa", b"bbb").unwrap();
        }
        assert!(!wb.should_write_to_engine());

        wb.put(b"aaa", b"bbb").unwrap();
        assert!(wb.should_write_to_engine());

        wb.clear();
        assert!(!wb.should_write_to_engine());
    }
}
