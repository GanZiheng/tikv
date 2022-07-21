// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use agatedb::{Agate, AgateIterator, IteratorOptions};
use bytes::Bytes;
use engine_traits::{
    Error, IterOptions, Iterable, Iterator, KvEngine, Peekable, ReadOptions, Result, SeekKey,
    SyncMutable, TabletAccessor, WriteOptions, CF_DEFAULT,
};

use crate::{
    db_vector::AgateDBVector,
    snapshot::AgateSnapshot,
    utils::{add_cf_prefix, get_cf_and_key},
    write_batch::AgateWriteBatch,
};

#[derive(Clone, Debug)]
pub struct AgateEngine {
    agate: Arc<Agate>,
    pub(crate) column_families: Vec<String>,
}

impl KvEngine for AgateEngine {
    type Snapshot = AgateSnapshot;

    fn snapshot(&self) -> Self::Snapshot {
        panic!()
    }
    fn sync(&self) -> Result<()> {
        panic!()
    }
    fn bad_downcast<T: 'static>(&self) -> &T {
        panic!()
    }
}

impl TabletAccessor<AgateEngine> for AgateEngine {
    fn for_each_opened_tablet(&self, f: &mut dyn FnMut(u64, u64, &AgateEngine)) {
        panic!()
    }

    fn is_single_engine(&self) -> bool {
        panic!()
    }
}

impl Peekable for AgateEngine {
    type DBVector = AgateDBVector;

    fn get_value_opt(&self, opts: &ReadOptions, key: &[u8]) -> Result<Option<Self::DBVector>> {
        panic!()
    }
    fn get_value_cf_opt(
        &self,
        opts: &ReadOptions,
        cf: &str,
        key: &[u8],
    ) -> Result<Option<Self::DBVector>> {
        panic!()
    }
}

impl SyncMutable for AgateEngine {
    fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let key = &add_cf_prefix(key, None);

        let mut txn = self.agate.new_transaction(true);
        txn.set(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
        txn.commit().map_err(|e| Error::Engine(e.to_string()))
    }

    fn put_cf(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        let key = &add_cf_prefix(key, Some(cf.to_string()));

        let mut txn = self.agate.new_transaction(true);
        txn.set(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
        txn.commit().map_err(|e| Error::Engine(e.to_string()))
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        let key = &add_cf_prefix(key, None);

        let mut txn = self.agate.new_transaction(true);
        txn.delete(Bytes::copy_from_slice(key));
        txn.commit().map_err(|e| Error::Engine(e.to_string()))
    }

    fn delete_cf(&self, cf: &str, key: &[u8]) -> Result<()> {
        let key = &add_cf_prefix(key, Some(cf.to_string()));

        let mut txn = self.agate.new_transaction(true);
        txn.delete(Bytes::copy_from_slice(key));
        txn.commit().map_err(|e| Error::Engine(e.to_string()))
    }

    fn delete_range(&self, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        panic!()
    }

    fn delete_range_cf(&self, cf: &str, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        panic!()
    }
}

impl Iterable for AgateEngine {
    type Iterator = AgateEngineIterator;

    fn iterator_opt(&self, opts: IterOptions) -> Result<Self::Iterator> {
        let txn = self.agate.new_transaction(false);

        let iter = txn.new_iterator(&IteratorOptions::default());

        Ok(AgateEngineIterator {
            iter,
            column_family: None,
        })
    }
    fn iterator_cf_opt(&self, cf: &str, opts: IterOptions) -> Result<Self::Iterator> {
        let txn = self.agate.new_transaction(false);

        let iter = txn.new_iterator(&IteratorOptions::default());

        Ok(AgateEngineIterator {
            iter,
            column_family: Some(cf.to_owned()),
        })
    }
}

pub struct AgateEngineIterator {
    iter: agatedb::Iterator,
    column_family: Option<String>,
}

impl Iterator for AgateEngineIterator {
    fn seek(&mut self, key: SeekKey<'_>) -> Result<bool> {
        match key {
            SeekKey::Start => {
                self.iter
                    .seek(&Bytes::from(add_cf_prefix(&[], self.column_family.clone())));

                Ok(self.valid()?)
            }
            SeekKey::End => {
                let seek_result = self.seek(SeekKey::Start)?;

                // No such key found.
                if !seek_result {
                    return Ok(false);
                }

                while self.valid()? {
                    self.next();
                }

                self.prev();
                Ok(self.valid()?)
            }
            SeekKey::Key(key) => {
                self.iter
                    .seek(&Bytes::from(add_cf_prefix(key, self.column_family.clone())));

                Ok(self.valid()?)
            }
        }
    }

    fn seek_for_prev(&mut self, key: SeekKey<'_>) -> Result<bool> {
        match key {
            SeekKey::Start => self.seek(SeekKey::Start),
            SeekKey::End => self.seek(SeekKey::End),
            SeekKey::Key(key) => {
                self.seek(SeekKey::Key(key))?;

                if self.key() != key {
                    self.prev();
                }

                Ok(self.valid()?)
            }
        }
    }

    fn prev(&mut self) -> Result<bool> {
        if !self.iter.valid() {
            return Err(Error::Engine("Iterator invalid".to_string()));
        }

        self.iter.prev();
        self.valid()
    }

    fn next(&mut self) -> Result<bool> {
        if !self.iter.valid() {
            return Err(Error::Engine("Iterator invalid".to_string()));
        }

        self.iter.next();
        self.valid()
    }

    fn key(&self) -> &[u8] {
        assert!(self.valid().unwrap());
        self.iter.item().key()
    }

    fn value(&self) -> &[u8] {
        assert!(self.valid().unwrap());
        self.iter.item().value()
    }

    fn valid(&self) -> Result<bool> {
        if !self.iter.valid() {
            return Ok(false);
        }

        let (cf_name, _) = get_cf_and_key(self.key());

        let cf_name_match = match &self.column_family {
            Some(cf) => &cf_name == cf,
            None => cf_name == CF_DEFAULT,
        };

        Ok(cf_name_match)
    }
}
