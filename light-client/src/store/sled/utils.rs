//! This modules provides type-safe interfaces over the `sled` API,
//! by taking care of (de)serializing keys and values with the
//! CBOR binary encoding.

use serde::{de::DeserializeOwned, Serialize};
use std::marker::PhantomData;

use crate::errors::{Error, ErrorKind};
use sled::{Db, Tree};

/// Provides a view over the database for storing a single value at the given prefix.
pub fn single<V>(db: &Db, prefix: impl Into<Vec<u8>>) -> SingleDb<V> {
    SingleDb::new(db, prefix)
}

/// Provides a view over the database for storing key/value pairs at the given prefix.
pub fn key_value<K, V>(db: &Db, prefix: impl Into<Vec<u8>>) -> KeyValueDb<K, V> {
    KeyValueDb::new(db.open_tree(prefix.into()).unwrap())
}

/// Provides a view over the database for storing a single value at the given prefix.
pub struct SingleDb<V>(KeyValueDb<(), V>);

impl<V> SingleDb<V> {
    pub fn new(db: &Db, prefix: impl Into<Vec<u8>>) -> Self {
        Self(KeyValueDb::new(db.open_tree(prefix.into()).unwrap()))
    }
}

impl<V> SingleDb<V>
where
    V: Serialize + DeserializeOwned,
{
    pub fn get(&self) -> Result<Option<V>, Error> {
        self.0.get(&())
    }

    pub fn set(&self, value: &V) -> Result<(), Error> {
        self.0.insert(&(), &value)
    }
}

/// Provides a view over the database for storing key/value pairs at the given prefix.
#[derive(Clone, Debug)]
pub struct KeyValueDb<K, V> {
    tree: Tree,
    marker: PhantomData<(K, V)>,
}

impl<K, V> KeyValueDb<K, V> {
    pub fn new(tree: Tree) -> Self {
        Self {
            tree,
            marker: PhantomData,
        }
    }
}

impl<K, V> KeyValueDb<K, V>
where
    K: Serialize,
    V: Serialize + DeserializeOwned,
{
    pub fn get(&self, key: &K) -> Result<Option<V>, Error> {
        let key_bytes = serde_cbor::to_vec(&key).map_err(|e| ErrorKind::Store.context(e))?;

        let value_bytes = self
            .tree
            .get(key_bytes)
            .map_err(|e| ErrorKind::Store.context(e))?;

        match value_bytes {
            Some(bytes) => {
                let value =
                    serde_cbor::from_slice(&bytes).map_err(|e| ErrorKind::Store.context(e))?;
                Ok(value)
            }
            None => Ok(None),
        }
    }

    pub fn contains_key(&self, key: &K) -> Result<bool, Error> {
        let key_bytes = serde_cbor::to_vec(&key).map_err(|e| ErrorKind::Store.context(e))?;

        let exists = self
            .tree
            .contains_key(key_bytes)
            .map_err(|e| ErrorKind::Store.context(e))?;

        Ok(exists)
    }

    pub fn insert(&self, key: &K, value: &V) -> Result<(), Error> {
        let key_bytes = serde_cbor::to_vec(&key).map_err(|e| ErrorKind::Store.context(e))?;
        let value_bytes = serde_cbor::to_vec(&value).map_err(|e| ErrorKind::Store.context(e))?;

        self.tree
            .insert(key_bytes, value_bytes)
            .map(|_| ())
            .map_err(|e| ErrorKind::Store.context(e))?;

        Ok(())
    }

    pub fn remove(&self, key: &K) -> Result<(), Error> {
        let key_bytes = serde_cbor::to_vec(&key).map_err(|e| ErrorKind::Store.context(e))?;

        self.tree
            .remove(key_bytes)
            .map_err(|e| ErrorKind::Store.context(e))?;

        Ok(())
    }

    pub fn iter(&self) -> impl DoubleEndedIterator<Item = V> {
        self.tree
            .iter()
            .flatten()
            .map(|(_, v)| serde_cbor::from_slice(&v))
            .flatten()
    }
}

// TODO: The test below is currently disabled because it fails on CI as we don't have
// access to `/tmp`. Need to figure out how to specify a proper temp dir.

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::types::Height;

//     #[test]
//     fn iter_next_back_returns_highest_height() {
//         const DB_PATH: &str = "/tmp/tendermint_light_client_sled_test/";
//         std::fs::remove_dir_all(DB_PATH).unwrap();
//         let db = sled::open(DB_PATH).unwrap();
//         let kv: KeyValueDb<Height, Height> = key_value("light_store/verified");

//         kv.insert(&db, &1, &1).unwrap();
//         kv.insert(&db, &589473798493, &589473798493).unwrap();
//         kv.insert(&db, &12342425, &12342425).unwrap();
//         kv.insert(&db, &4, &4).unwrap();

//         let mut iter = kv.iter(&db);
//         assert_eq!(iter.next_back(), Some(589473798493));
//         assert_eq!(iter.next_back(), Some(12342425));
//         assert_eq!(iter.next_back(), Some(4));
//         assert_eq!(iter.next_back(), Some(1));
//         assert_eq!(iter.next_back(), None);
//     }
// }
