// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::CFNamesExt;

use crate::engine::AgateEngine;

impl CFNamesExt for AgateEngine {
    fn cf_names(&self) -> Vec<&str> {
        self.cf_names.iter().map(|cf| cf.as_str()).collect()
    }
}
