// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use agatedb::{
    opt::build_table_options, table::TableIterator, value::VALUE_DELETE, Agate, AgateIterator,
    AgateOptions, Table, TableBuilder, TableOptions, Value,
};
use bytes::Bytes;
use engine_traits::{
    CfName, ExternalSstFileInfo, IterOptions, Iterable, Iterator, Result, SeekKey,
    SstCompressionType, SstExt, SstReader, SstWriter, SstWriterBuilder, CF_DEFAULT,
};

use crate::{
    engine::AgateEngine,
    utils::{add_cf_prefix, get_cf_and_key},
};

const SST_USER_META: u8 = 1 << 2;

impl SstExt for AgateEngine {
    type SstReader = AgateSstReader;
    type SstWriter = AgateSstWriter;
    type SstWriterBuilder = AgateSstWriterBuilder;
}

pub struct AgateSstReader {
    table: Table,
}

impl SstReader for AgateSstReader {
    fn open(path: &str) -> Result<Self> {
        let path = Path::new(path);
        let opts = build_table_options(&AgateOptions::default());
        let table =
            Table::open(path, opts).map_err(|e| engine_traits::Error::Engine(e.to_string()))?;

        Ok(Self { table })
    }
    fn verify_checksum(&self) -> Result<()> {
        self.table
            .inner
            .verify_checksum()
            .map_err(|e| engine_traits::Error::Engine(e.to_string()))
    }
    fn iter(&self) -> Self::Iterator {
        // TODO: What is the cf name?
        self.iterator_opt(IterOptions::default()).unwrap()
    }
}

impl Iterable for AgateSstReader {
    type Iterator = AgateSstReaderIterator;

    fn iterator_opt(&self, opts: IterOptions) -> Result<Self::Iterator> {
        let iter = self.table.new_iterator(0);

        Ok(AgateSstReaderIterator {
            iter,
            opts,
            cf_name: None,
            current_value: Value::default(),
        })
    }
    fn iterator_cf_opt(&self, cf: &str, opts: IterOptions) -> Result<Self::Iterator> {
        let iter = self.table.new_iterator(0);

        Ok(AgateSstReaderIterator {
            iter,
            opts,
            cf_name: Some(cf.to_string()),
            current_value: Value::default(),
        })
    }
}

pub struct AgateSstReaderIterator {
    iter: TableIterator,
    opts: IterOptions,
    cf_name: Option<String>,
    current_value: agatedb::Value,
}

impl AgateSstReaderIterator {
    fn update(&mut self, is_forward: bool) -> Result<bool> {
        loop {
            if !self.iter.valid() {
                return Ok(false);
            }

            let (cf_name, key) = get_cf_and_key(self.iter.key());

            let cf_name_match = match &self.cf_name {
                Some(cf) => &cf_name == cf,
                None => cf_name == CF_DEFAULT,
            };

            if !cf_name_match {
                return Ok(false);
            }

            let value = self.iter.value();
            if value.meta & VALUE_DELETE != 0 {
                if is_forward {
                    self.iter.next();
                } else {
                    self.iter.prev();
                }

                continue;
            }

            self.current_value = value;
            return Ok(true);
        }
    }
}

impl Iterator for AgateSstReaderIterator {
    fn seek(&mut self, key: SeekKey<'_>) -> Result<bool> {
        match key {
            SeekKey::Start => {
                self.iter
                    .seek(&Bytes::from(add_cf_prefix(&[], self.cf_name.clone())));

                self.update(true)
            }
            SeekKey::End => {
                let valid = self.seek(SeekKey::Start)?;

                if !valid {
                    return Ok(false);
                }

                loop {
                    let valid = self.next()?;
                    if !valid {
                        break;
                    }
                }

                self.iter.prev();
                self.update(false)
            }
            SeekKey::Key(key) => {
                self.iter
                    .seek(&Bytes::from(add_cf_prefix(key, self.cf_name.clone())));

                self.update(true)
            }
        }
    }
    fn seek_for_prev(&mut self, key: SeekKey<'_>) -> Result<bool> {
        match key {
            SeekKey::Start => self.seek(SeekKey::Start),
            SeekKey::End => self.seek(SeekKey::End),
            SeekKey::Key(key) => {
                let valid = self.seek(SeekKey::Key(key))?;

                if !self.iter.valid() {
                    self.seek(SeekKey::End)
                } else {
                    if self.key() != key {
                        self.prev();
                    }

                    self.update(false)
                }
            }
        }
    }

    fn prev(&mut self) -> Result<bool> {
        if !self.iter.valid() {
            return Err(engine_traits::Error::Engine("Iterator invalid".to_string()));
        }

        self.iter.prev();
        self.update(false)
    }
    fn next(&mut self) -> Result<bool> {
        if !self.iter.valid() {
            return Err(engine_traits::Error::Engine("Iterator invalid".to_string()));
        }

        self.iter.next();
        self.update(true)
    }

    fn key(&self) -> &[u8] {
        assert!(self.valid().unwrap());
        get_cf_and_key(self.iter.key()).1
    }
    fn value(&self) -> &[u8] {
        assert!(self.valid().unwrap());
        &self.current_value.value
    }

    fn valid(&self) -> Result<bool> {
        if !self.iter.valid() {
            return Ok(false);
        }

        let (cf_name, key) = get_cf_and_key(self.iter.key());

        let cf_name_match = match &self.cf_name {
            Some(cf) => &cf_name == cf,
            None => cf_name == CF_DEFAULT,
        };

        if !cf_name_match {
            return Ok(false);
        }

        Ok(true)
    }
}

pub struct AgateSstWriter {
    builder: TableBuilder,
    cf_name: Option<String>,
    path: PathBuf,
    last_key: Bytes,
}

impl SstWriter for AgateSstWriter {
    type ExternalSstFileInfo = AgateExternalSstFileInfo;
    type ExternalSstFileReader = AgateExternalSstFileReader;

    fn put(&mut self, key: &[u8], val: &[u8]) -> Result<()> {
        let key = Bytes::from(add_cf_prefix(key, self.cf_name.clone()));
        if !self.last_key.is_empty() && key <= self.last_key {
            return Err(engine_traits::Error::Engine("Key not in order".to_string()));
        }
        self.last_key = key.clone();

        let value = Value::new_with_meta(Bytes::copy_from_slice(val), 0, SST_USER_META);

        self.builder.add(&key, &value, 0);
        Ok(())
    }
    fn delete(&mut self, key: &[u8]) -> Result<()> {
        let key = Bytes::from(add_cf_prefix(key, self.cf_name.clone()));
        if !self.last_key.is_empty() && key <= self.last_key {
            return Err(engine_traits::Error::Engine("Key not in order".to_string()));
        }
        self.last_key = key.clone();

        let value = Value::new_with_meta(Bytes::new(), VALUE_DELETE, SST_USER_META);

        self.builder.add(&key, &value, 0);
        Ok(())
    }
    fn file_size(&mut self) -> u64 {
        self.builder.estimated_size() as u64
    }
    fn finish(self) -> Result<Self::ExternalSstFileInfo> {
        let table =
            Table::create(&self.path, self.builder.finish(), TableOptions::default()).unwrap();
        table.mark_save();

        Ok(AgateExternalSstFileInfo {
            path: self.path,
            table,
        })
    }
    fn finish_read(self) -> Result<(Self::ExternalSstFileInfo, Self::ExternalSstFileReader)> {
        panic!()
    }
}

pub struct AgateSstWriterBuilder {
    agate: Option<Arc<Agate>>,
    cf_name: Option<String>,
    in_memory: bool,
    compression_type: Option<SstCompressionType>,
    compression_level: i32,
}

impl SstWriterBuilder<AgateEngine> for AgateSstWriterBuilder {
    fn new() -> Self {
        AgateSstWriterBuilder {
            agate: None,
            cf_name: None,
            in_memory: false,
            compression_type: None,
            compression_level: 0,
        }
    }
    fn set_db(mut self, db: &AgateEngine) -> Self {
        self.agate = Some(db.agate.clone());
        self
    }
    fn set_cf(mut self, cf: &str) -> Self {
        self.cf_name = Some(cf.to_string());
        self
    }
    fn set_in_memory(mut self, in_memory: bool) -> Self {
        self.in_memory = in_memory;
        self
    }
    fn set_compression_type(mut self, compression: Option<SstCompressionType>) -> Self {
        self.compression_type = compression;
        self
    }
    fn set_compression_level(mut self, level: i32) -> Self {
        self.compression_level = level;
        self
    }

    fn build(self, path: &str) -> Result<AgateSstWriter> {
        let builder = TableBuilder::new(TableOptions::default());
        Ok(AgateSstWriter {
            builder,
            cf_name: self.cf_name,
            path: PathBuf::from(path),
            last_key: Bytes::new(),
        })
    }
}

pub struct AgateExternalSstFileInfo {
    table: Table,
    path: PathBuf,
}

impl ExternalSstFileInfo for AgateExternalSstFileInfo {
    fn new() -> Self {
        panic!()
    }
    fn file_path(&self) -> PathBuf {
        self.path.clone()
    }
    fn smallest_key(&self) -> &[u8] {
        let (_, key) = get_cf_and_key(self.table.smallest());
        key
    }
    fn largest_key(&self) -> &[u8] {
        let (_, key) = get_cf_and_key(self.table.biggest());
        key
    }
    fn sequence_number(&self) -> u64 {
        panic!()
    }
    fn file_size(&self) -> u64 {
        self.table.size()
    }
    fn num_entries(&self) -> u64 {
        self.table.inner.key_count() as u64
    }
}

pub struct AgateExternalSstFileReader;

impl std::io::Read for AgateExternalSstFileReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        panic!()
    }
}
