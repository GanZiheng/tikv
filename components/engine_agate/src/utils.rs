// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::CF_DEFAULT;

// SIMPLE design, just use `DELIMITER` as the delimiter to seperate the name of column family
// and the original key. If we want to insert a key into a sepecified column family, we will
// use the format `${CF_NAME}${DELIMITER}${KEY}` to represent the key.
static DELIMITER: &str = "@!@";

pub fn add_cf_prefix(key: &[u8], cf_name: Option<String>) -> Vec<u8> {
    let mut cf_name = match cf_name {
        Some(cf_name) => cf_name,
        None => CF_DEFAULT.to_owned(),
    };

    cf_name += DELIMITER;

    let mut cf_name_vec = cf_name.as_bytes();
    vec![cf_name_vec, key].concat()
}

pub fn get_cf_and_key(key_with_cf: &[u8]) -> (String, Vec<u8>) {
    let key_with_cf = std::str::from_utf8(key_with_cf).unwrap();

    let mut key_vec = key_with_cf.split(DELIMITER).collect::<Vec<&str>>();

    let cf_name = key_vec.remove(0).to_string();
    let key = key_vec.concat().as_bytes().to_vec();

    (cf_name, key)
}

#[cfg(test)]
mod tests {
    use engine_traits::CF_DEFAULT;

    use super::{add_cf_prefix, get_cf_and_key};

    #[test]
    fn simple_add_cf_prefix() {
        let key = "key".as_bytes();
        let key_default = "default@!@key".as_bytes();
        let key_cf = "cf@!@key".as_bytes();

        assert_eq!(&add_cf_prefix(key, None), key_default);
        assert_eq!(&add_cf_prefix(key, Some("cf".to_string())), key_cf);
    }

    #[test]
    fn simple_get_cf_and_key() {
        let key = "key".as_bytes();

        let key_with_default_cf = add_cf_prefix(key, None);
        assert_eq!(
            get_cf_and_key(&key_with_default_cf),
            (CF_DEFAULT.to_string(), key.to_vec())
        );

        let cf = "cf".to_string();

        let key_with_cf = add_cf_prefix(key, Some(cf.clone()));
        assert_eq!(get_cf_and_key(&key_with_cf), (cf, key.to_vec()));
    }
}
