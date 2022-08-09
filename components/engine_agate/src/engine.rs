// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::HashSet,
    iter::FromIterator,
    path::{self, Path, PathBuf},
    sync::Arc,
};

use agatedb::{Agate, AgateIterator, AgateOptions, IteratorOptions};
use bytes::Bytes;
use engine_traits::{
    Error, IterOptions, Iterable, Iterator, KvEngine, MiscExt, Peekable, RaftEngine, ReadOptions,
    Result, SeekKey, SyncMutable, TabletAccessor, WriteOptions, CF_DEFAULT,
};

use crate::{
    db_vector::AgateDBVector,
    snapshot::AgateSnapshot,
    utils::{add_cf_prefix, get_cf_and_key},
    write_batch::AgateWriteBatch,
};

#[derive(Clone, Debug)]
pub struct AgateEngine {
    pub(crate) agate: Arc<Agate>,
    pub(crate) cf_names: HashSet<String>,
    pub(crate) path: PathBuf,
}

impl AgateEngine {
    pub fn new(path: &Path, cfs: Vec<String>) -> Self {
        let mut agate_opts = AgateOptions {
            dir: path.to_path_buf(),
            value_dir: path.to_path_buf(),
            ..Default::default()
        };

        AgateEngine {
            agate: Arc::new(agate_opts.open().unwrap()),
            cf_names: HashSet::from_iter([vec![CF_DEFAULT.to_string()], cfs].concat().into_iter()),
            path: path.to_path_buf(),
        }
    }

    pub fn check_cf_exist(&self, cf: &str) -> Result<()> {
        if !self.cf_names.contains(cf) {
            Err(engine_traits::Error::CFName(cf.to_string()))
        } else {
            Ok(())
        }
    }
}

impl KvEngine for AgateEngine {
    type Snapshot = AgateSnapshot;

    fn snapshot(&self) -> Self::Snapshot {
        AgateSnapshot::new(self)
    }
    fn sync(&self) -> Result<()> {
        self.sync_wal()
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
        let key = add_cf_prefix(key, None);

        let mut txn = self.agate.new_transaction(false);

        match txn.get(&Bytes::from(key)) {
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

        let key = add_cf_prefix(key, Some(cf.to_string()));

        let mut txn = self.agate.new_transaction(false);

        match txn.get(&Bytes::from(key)) {
            Ok(item) => Ok(Some(AgateDBVector::from_raw(item.value()))),
            Err(e) => match e {
                agatedb::Error::KeyNotFound(()) => Ok(None),
                _ => Err(engine_traits::Error::Engine(e.to_string())),
            },
        }
    }
}

impl SyncMutable for AgateEngine {
    fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let key = add_cf_prefix(key, None);

        let mut txn = self.agate.new_transaction(true);
        txn.set(Bytes::from(key), Bytes::copy_from_slice(value))
            .map_err(|e| engine_traits::Error::Engine(e.to_string()))?;
        txn.commit()
            .map_err(|e| engine_traits::Error::Engine(e.to_string()))
    }

    fn put_cf(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        self.check_cf_exist(cf)?;

        let key = add_cf_prefix(key, Some(cf.to_string()));

        let mut txn = self.agate.new_transaction(true);
        txn.set(Bytes::from(key), Bytes::copy_from_slice(value))
            .map_err(|e| engine_traits::Error::Engine(e.to_string()))?;
        txn.commit()
            .map_err(|e| engine_traits::Error::Engine(e.to_string()))
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        let key = add_cf_prefix(key, None);

        let mut txn = self.agate.new_transaction(true);
        txn.delete(Bytes::from(key))
            .map_err(|e| engine_traits::Error::Engine(e.to_string()))?;
        txn.commit()
            .map_err(|e| engine_traits::Error::Engine(e.to_string()))
    }

    fn delete_cf(&self, cf: &str, key: &[u8]) -> Result<()> {
        self.check_cf_exist(cf)?;

        let key = add_cf_prefix(key, Some(cf.to_string()));

        let mut txn = self.agate.new_transaction(true);
        txn.delete(Bytes::from(key))
            .map_err(|e| engine_traits::Error::Engine(e.to_string()))?;
        txn.commit()
            .map_err(|e| engine_traits::Error::Engine(e.to_string()))
    }

    fn delete_range(&self, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        if end_key < begin_key {
            return Err(engine_traits::Error::Engine(
                "end_key < begin_key".to_string(),
            ));
        }

        let mut txn = self.agate.new_transaction(true);

        self.scan(begin_key, end_key, false, |key, _| {
            let key = &add_cf_prefix(key, None);
            txn.delete(Bytes::copy_from_slice(key))
                .map_err(|e| engine_traits::Error::Engine(e.to_string()))?;

            Ok(true)
        });

        txn.commit()
            .map_err(|e| engine_traits::Error::Engine(e.to_string()))
    }

    fn delete_range_cf(&self, cf: &str, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        if end_key < begin_key {
            return Err(engine_traits::Error::Engine(
                "end_key < begin_key".to_string(),
            ));
        }

        self.check_cf_exist(cf)?;

        let mut txn = self.agate.new_transaction(true);

        self.scan_cf(cf, begin_key, end_key, false, |key, _| {
            let key = add_cf_prefix(key, Some(cf.to_string()));
            txn.delete(Bytes::from(key))
                .map_err(|e| engine_traits::Error::Engine(e.to_string()))?;

            Ok(true)
        });

        txn.commit()
            .map_err(|e| engine_traits::Error::Engine(e.to_string()))
    }
}

impl Iterable for AgateEngine {
    type Iterator = AgateEngineIterator;

    fn iterator_opt(&self, opts: IterOptions) -> Result<Self::Iterator> {
        let txn = self.agate.new_transaction(false);

        let iter = txn.new_iterator(&IteratorOptions::default());

        Ok(AgateEngineIterator {
            iter,
            opts,
            cf_name: None,
        })
    }
    fn iterator_cf_opt(&self, cf: &str, opts: IterOptions) -> Result<Self::Iterator> {
        self.check_cf_exist(cf)?;

        let txn = self.agate.new_transaction(false);

        let iter = txn.new_iterator(&IteratorOptions::default());

        Ok(AgateEngineIterator {
            iter,
            opts,
            cf_name: Some(cf.to_owned()),
        })
    }
}

pub struct AgateEngineIterator {
    pub(crate) iter: agatedb::Iterator,
    pub(crate) opts: IterOptions,
    pub(crate) cf_name: Option<String>,
}

impl Iterator for AgateEngineIterator {
    fn seek(&mut self, key: SeekKey<'_>) -> Result<bool> {
        match key {
            SeekKey::Start => {
                self.iter
                    .seek(&Bytes::from(add_cf_prefix(&[], self.cf_name.clone())));

                self.valid()
            }
            SeekKey::End => {
                let seek_result = self.seek(SeekKey::Start)?;

                // No such key found.
                if !seek_result {
                    return Ok(false);
                }

                assert!(self.valid()?);
                let mut last_key = vec![];

                while self.valid()? {
                    let key = self.key();
                    last_key.clear();
                    last_key.extend_from_slice(key);
                    self.next();
                }

                self.seek(SeekKey::Key(&last_key))
            }
            SeekKey::Key(key) => {
                self.iter
                    .seek(&Bytes::from(add_cf_prefix(key, self.cf_name.clone())));

                self.valid()
            }
        }
    }

    fn seek_for_prev(&mut self, key: SeekKey<'_>) -> Result<bool> {
        match key {
            SeekKey::Start => self.seek(SeekKey::Start),
            SeekKey::End => self.seek(SeekKey::End),
            SeekKey::Key(key) => {
                let valid = self.seek(SeekKey::Key(key))?;

                if !valid {
                    // TODO: Consider exist_key < seek_key < upper_bound_key.
                    return self.seek_to_last();
                }

                if self.key() != key {
                    self.prev();
                }

                self.valid()
            }
        }
    }

    fn prev(&mut self) -> Result<bool> {
        if !self.iter.valid() {
            return Err(engine_traits::Error::Engine("Iterator invalid".to_string()));
        }

        self.iter.prev();
        self.valid()
    }

    fn next(&mut self) -> Result<bool> {
        if !self.iter.valid() {
            return Err(engine_traits::Error::Engine("Iterator invalid".to_string()));
        }

        self.iter.next();
        self.valid()
    }

    fn key(&self) -> &[u8] {
        assert!(self.valid().unwrap());
        get_cf_and_key(self.iter.item().key()).1
    }

    fn value(&self) -> &[u8] {
        assert!(self.valid().unwrap());
        self.iter.item().value()
    }

    fn valid(&self) -> Result<bool> {
        if !self.iter.valid() {
            return Ok(false);
        }

        let (cf_name, key) = get_cf_and_key(self.iter.item().key());

        let cf_name_match = match &self.cf_name {
            Some(cf) => &cf_name == cf,
            None => cf_name == CF_DEFAULT,
        };

        if !cf_name_match {
            return Ok(false);
        }

        if self.opts.lower_bound().is_some() {
            let lower = self.opts.lower_bound().unwrap();
            if !lower.is_empty() && key < lower {
                return Ok(false);
            }
        }
        if self.opts.upper_bound().is_some() {
            let upper = self.opts.upper_bound().unwrap();
            if !upper.is_empty() && key >= upper {
                return Ok(false);
            }
        }

        Ok(true)
    }
}
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use engine_traits::{Iterable, KvEngine, Peekable, SyncMutable};
    use kvproto::metapb::Region;
    use tempfile::Builder;

    use crate::{AgateEngine, AgateSnapshot};

    #[test]
    fn test_base() {
        let path = Builder::new().prefix("var").tempdir().unwrap();
        let cf = "cf";
        let engine = AgateEngine::new(path.path(), vec![cf.to_string()]);

        let mut r = Region::default();
        r.set_id(10);

        let key = b"key";
        engine.put_msg(key, &r).unwrap();
        engine.put_msg_cf(cf, key, &r).unwrap();

        let snap = engine.snapshot();

        let mut r1: Region = engine.get_msg(key).unwrap().unwrap();
        assert_eq!(r, r1);
        let r1_cf: Region = engine.get_msg_cf(cf, key).unwrap().unwrap();
        assert_eq!(r, r1_cf);

        let mut r2: Region = snap.get_msg(key).unwrap().unwrap();
        assert_eq!(r, r2);
        let r2_cf: Region = snap.get_msg_cf(cf, key).unwrap().unwrap();
        assert_eq!(r, r2_cf);

        r.set_id(11);
        engine.put_msg(key, &r).unwrap();
        r1 = engine.get_msg(key).unwrap().unwrap();
        r2 = snap.get_msg(key).unwrap().unwrap();
        assert_ne!(r1, r2);

        let b: Option<Region> = engine.get_msg(b"missing_key").unwrap();
        assert!(b.is_none());
    }

    #[test]
    fn test_peekable() {
        let path = Builder::new().prefix("var").tempdir().unwrap();
        let cf = "cf";
        let engine = AgateEngine::new(path.path(), vec![cf.to_string()]);

        engine.put(b"k1", b"v1").unwrap();
        engine.put_cf(cf, b"k1", b"v2").unwrap();

        assert_eq!(&*engine.get_value(b"k1").unwrap().unwrap(), b"v1");
        assert!(engine.get_value_cf("foo", b"k1").is_err());
        assert_eq!(&*engine.get_value_cf(cf, b"k1").unwrap().unwrap(), b"v2");
    }

    #[test]
    fn test_scan() {
        let path = Builder::new().prefix("var").tempdir().unwrap();
        let cf = "cf";
        let engine = AgateEngine::new(path.path(), vec![cf.to_string()]);

        engine.put(b"a1", b"v1").unwrap();
        engine.put(b"a2", b"v2").unwrap();
        engine.put_cf(cf, b"a1", b"v1").unwrap();
        engine.put_cf(cf, b"a2", b"v22").unwrap();

        let mut data = vec![];
        engine
            .scan(b"", &[0xFF, 0xFF], false, |key, value| {
                data.push((key.to_vec(), value.to_vec()));
                Ok(true)
            })
            .unwrap();
        assert_eq!(
            data,
            vec![
                (b"a1".to_vec(), b"v1".to_vec()),
                (b"a2".to_vec(), b"v2".to_vec()),
            ]
        );
        data.clear();

        engine
            .scan_cf(cf, b"", &[0xFF, 0xFF], false, |key, value| {
                data.push((key.to_vec(), value.to_vec()));
                Ok(true)
            })
            .unwrap();
        assert_eq!(
            data,
            vec![
                (b"a1".to_vec(), b"v1".to_vec()),
                (b"a2".to_vec(), b"v22".to_vec()),
            ]
        );
        data.clear();

        let pair = engine.seek(b"a1").unwrap().unwrap();
        assert_eq!(pair, (b"a1".to_vec(), b"v1".to_vec()));
        assert!(engine.seek(b"a3").unwrap().is_none());
        let pair_cf = engine.seek_cf(cf, b"a1").unwrap().unwrap();
        assert_eq!(pair_cf, (b"a1".to_vec(), b"v1".to_vec()));
        assert!(engine.seek_cf(cf, b"a3").unwrap().is_none());

        let mut index = 0;
        engine
            .scan(b"", &[0xFF, 0xFF], false, |key, value| {
                data.push((key.to_vec(), value.to_vec()));
                index += 1;
                Ok(index != 1)
            })
            .unwrap();

        assert_eq!(data.len(), 1);

        let snap = AgateSnapshot::new(&engine);

        engine.put(b"a3", b"v3").unwrap();
        assert!(engine.seek(b"a3").unwrap().is_some());

        let pair = snap.seek(b"a1").unwrap().unwrap();
        assert_eq!(pair, (b"a1".to_vec(), b"v1".to_vec()));
        assert!(snap.seek(b"a3").unwrap().is_none());

        data.clear();

        snap.scan(b"", &[0xFF, 0xFF], false, |key, value| {
            data.push((key.to_vec(), value.to_vec()));
            Ok(true)
        })
        .unwrap();

        assert_eq!(data.len(), 2);
    }

    #[test]
    fn test_delete_range() {
        let path = Builder::new().prefix("var").tempdir().unwrap();
        let cf = "cf";
        let engine = AgateEngine::new(path.path(), vec![cf.to_string()]);

        engine.put(b"a1", b"v1").unwrap();
        engine.put(b"a2", b"v2").unwrap();
        engine.put_cf(cf, b"a1", b"v1").unwrap();
        engine.put_cf(cf, b"a2", b"v22").unwrap();

        // Delete a1 in default cf.
        engine.delete_range(b"", b"a2").unwrap();

        let mut data = vec![];
        engine
            .scan(b"", &[0xFF, 0xFF], false, |key, value| {
                data.push((key.to_vec(), value.to_vec()));
                Ok(true)
            })
            .unwrap();
        assert_eq!(data, vec![(b"a2".to_vec(), b"v2".to_vec()),]);
        data.clear();

        engine
            .scan_cf(cf, b"", &[0xFF, 0xFF], false, |key, value| {
                data.push((key.to_vec(), value.to_vec()));
                Ok(true)
            })
            .unwrap();
        assert_eq!(
            data,
            vec![
                (b"a1".to_vec(), b"v1".to_vec()),
                (b"a2".to_vec(), b"v22".to_vec()),
            ]
        );
        data.clear();

        // Delete a2 in cf.
        engine.delete_range_cf(cf, b"a2", &[0xFF, 0xFF]).unwrap();

        engine
            .scan(b"", &[0xFF, 0xFF], false, |key, value| {
                data.push((key.to_vec(), value.to_vec()));
                Ok(true)
            })
            .unwrap();
        assert_eq!(data, vec![(b"a2".to_vec(), b"v2".to_vec()),]);
        data.clear();

        engine
            .scan_cf(cf, b"", &[0xFF, 0xFF], false, |key, value| {
                data.push((key.to_vec(), value.to_vec()));
                Ok(true)
            })
            .unwrap();
        assert_eq!(data, vec![(b"a1".to_vec(), b"v1".to_vec()),]);
    }
}
