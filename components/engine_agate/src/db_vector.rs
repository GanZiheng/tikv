// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::ops::Deref;

use engine_traits::DBVector;

#[derive(Debug)]
pub struct AgateDBVector(Vec<u8>);

impl AgateDBVector {
    pub fn from_raw(raw: &[u8]) -> AgateDBVector {
        AgateDBVector(Vec::from(raw))
    }
}

impl DBVector for AgateDBVector {}

impl Deref for AgateDBVector {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.0
    }
}

impl<'a> PartialEq<&'a [u8]> for AgateDBVector {
    fn eq(&self, rhs: &&[u8]) -> bool {
        **rhs == **self
    }
}
