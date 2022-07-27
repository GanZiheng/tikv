// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{collections::HashSet, fmt::Debug, ops::Deref, sync::Arc};

use agatedb::{Agate, IteratorOptions};
use bytes::Bytes;
use engine_traits::{
    CFNamesExt, IterOptions, Iterable, Iterator, Peekable, ReadOptions, Result, SeekKey, Snapshot,
};

use crate::{
    db_vector::AgateDBVector, engine::AgateEngine, utils::add_cf_prefix, AgateEngineIterator,
};

#[derive(Clone)]
pub struct AgateSnapshot {
    txn: agatedb::Transaction,
    cf_names: HashSet<String>,
}

impl Debug for AgateSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AgateSnapshot")
            .field("txn read ts", &self.txn.get_read_ts())
            .field("cf_names", &self.cf_names)
            .finish()
    }
}

impl AgateSnapshot {
    pub fn new(engine: &AgateEngine) -> Self {
        let txn = engine.agate.new_transaction(false);
        let cf_names = engine.cf_names().iter().map(|x| x.to_string()).collect();

        AgateSnapshot { txn, cf_names }
    }

    pub fn check_cf_exist(&self, cf: &str) -> Result<()> {
        if !self.cf_names.contains(cf) {
            Err(engine_traits::Error::CFName(cf.to_string()))
        } else {
            Ok(())
        }
    }
}

impl Snapshot for AgateSnapshot {
    fn cf_names(&self) -> Vec<&str> {
        // TODO: Check.
        self.cf_names.iter().map(|s| s.as_str()).collect()
    }
}

impl Peekable for AgateSnapshot {
    type DBVector = AgateDBVector;

    fn get_value_opt(&self, opts: &ReadOptions, key: &[u8]) -> Result<Option<Self::DBVector>> {
        let key = &add_cf_prefix(key, None);

        match self.txn.get(&Bytes::copy_from_slice(key)) {
            Ok(item) => Ok(Some(AgateDBVector::from_raw(item.value()))),
            Err(e) => match e {
                agatedb::Error::KeyNotFound(()) => Ok(None),
                _ => Err(engine_traits::Error::Engine(e.to_string())),
            },
        }
    }
    fn get_value_cf_opt(
        &self,
        opts: &ReadOptions,
        cf: &str,
        key: &[u8],
    ) -> Result<Option<Self::DBVector>> {
        self.check_cf_exist(cf)?;

        let key = &add_cf_prefix(key, Some(cf.to_string()));

        match self.txn.get(&Bytes::copy_from_slice(key)) {
            Ok(item) => Ok(Some(AgateDBVector::from_raw(item.value()))),
            Err(e) => match e {
                agatedb::Error::KeyNotFound(()) => Ok(None),
                _ => Err(engine_traits::Error::Engine(e.to_string())),
            },
        }
    }
}

impl Iterable for AgateSnapshot {
    type Iterator = AgateEngineIterator;

    fn iterator_opt(&self, opts: IterOptions) -> Result<Self::Iterator> {
        let iter = self.txn.new_iterator(&IteratorOptions::default());

        Ok(AgateEngineIterator {
            iter,
            opts,
            cf_name: None,
        })
    }
    fn iterator_cf_opt(&self, cf: &str, opts: IterOptions) -> Result<Self::Iterator> {
        self.check_cf_exist(cf)?;

        let iter = self.txn.new_iterator(&IteratorOptions::default());

        Ok(AgateEngineIterator {
            iter,
            opts,
            cf_name: Some(cf.to_owned()),
        })
    }
}

pub struct AgateSnapshotIterator;

impl Iterator for AgateSnapshotIterator {
    fn seek(&mut self, key: SeekKey<'_>) -> Result<bool> {
        panic!()
    }
    fn seek_for_prev(&mut self, key: SeekKey<'_>) -> Result<bool> {
        panic!()
    }

    fn prev(&mut self) -> Result<bool> {
        panic!()
    }
    fn next(&mut self) -> Result<bool> {
        panic!()
    }

    fn key(&self) -> &[u8] {
        panic!()
    }
    fn value(&self) -> &[u8] {
        panic!()
    }

    fn valid(&self) -> Result<bool> {
        panic!()
    }
}
