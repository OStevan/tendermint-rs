pub mod utils;

use crate::{
    store::sled::utils::*,
    types::{Height, LightBlock},
};

use super::{LightStore, Status};
use sled::Db;

const UNVERIFIED_PREFIX: &str = "light_store/unverified";
const VERIFIED_PREFIX: &str = "light_store/verified";
const TRUSTED_PREFIX: &str = "light_store/trusted";
const FAILED_PREFIX: &str = "light_store/failed";

/// Persistent store backed by an on-disk `sled` database.
#[derive(Debug, Clone)]
pub struct SledStore {
    unverified_db: KeyValueDb<Height, LightBlock>,
    verified_db: KeyValueDb<Height, LightBlock>,
    trusted_db: KeyValueDb<Height, LightBlock>,
    failed_db: KeyValueDb<Height, LightBlock>,
}

impl SledStore {
    pub fn new(db: &Db) -> Self {
        Self {
            unverified_db: key_value(&db, UNVERIFIED_PREFIX),
            verified_db: key_value(&db, VERIFIED_PREFIX),
            trusted_db: key_value(&db, TRUSTED_PREFIX),
            failed_db: key_value(&db, FAILED_PREFIX),
        }
    }

    fn db(&self, status: Status) -> &KeyValueDb<Height, LightBlock> {
        match status {
            Status::Unverified => &self.unverified_db,
            Status::Verified => &self.verified_db,
            Status::Trusted => &self.trusted_db,
            Status::Failed => &self.failed_db,
        }
    }
}

impl LightStore for SledStore {
    fn get(&self, height: Height, status: Status) -> Option<LightBlock> {
        self.db(status).get(&height).ok().flatten()
    }

    fn update(&mut self, light_block: &LightBlock, status: Status) {
        let height = light_block.height();

        for other in Status::iter() {
            if status != *other {
                self.db(*other).remove(&height).ok();
            }
        }

        self.db(status).insert(&height, light_block).ok();
    }

    fn insert(&mut self, light_block: LightBlock, status: Status) {
        self.db(status)
            .insert(&light_block.height(), &light_block)
            .ok();
    }

    fn remove(&mut self, height: Height, status: Status) {
        self.db(status).remove(&height).ok();
    }

    fn latest(&self, status: Status) -> Option<LightBlock> {
        self.db(status)
            .iter()
            .max_by(|first, second| first.height().cmp(&second.height()))
    }

    fn all(&self, status: Status) -> Box<dyn Iterator<Item = LightBlock>> {
        Box::new(self.db(status).iter())
    }
}
