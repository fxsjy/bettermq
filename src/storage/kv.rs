use std::cmp::Ordering;
use strum_macros::Display;
use tracing::info;

pub enum DbKind {
    SLED,
    ROCKSDB,
}

#[derive(Debug, Display)]
pub enum KvError {
    NotFound(String),
    IoError(String),
}

pub trait KvStore: Send + Sync + 'static {
    fn get(&self, key: &Vec<u8>) -> Result<Vec<u8>, KvError>;
    fn set(&self, key: &Vec<u8>, value: Vec<u8>) -> Result<(), KvError>;
    fn scan(
        &self,
        start: &Vec<u8>,
        end: &Vec<u8>,
        limit: u32,
        items: &mut Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<(), KvError>;
    fn remove(&self, key: &Vec<u8>) -> Result<(), KvError>;
    fn max_key(&self) -> Result<Vec<u8>, KvError>;
}

#[derive(Debug)]
struct SledKv {
    db: sled::Db,
}

#[derive(Debug)]
struct RocksDBKv {
    db: rocksdb::DBWithThreadMode<rocksdb::MultiThreaded>,
}

pub fn new_kvstore(dbkind: DbKind, dir: String) -> Result<Box<dyn KvStore>, KvError> {
    match dbkind {
        DbKind::SLED => {
            info!("open db: {:}", dir);
            let db = sled::open(dir);
            match db {
                Ok(idb) => {
                    let kvs = Box::new(SledKv { db: idb });
                    Ok(kvs)
                }
                Err(err) => Err(KvError::IoError(err.to_string())),
            }
        }
        DbKind::ROCKSDB => {
            info!("open db (leveldb) {:}", dir);
            let mut opts = rocksdb::Options::default();
            opts.create_if_missing(true);
            let db = rocksdb::DBWithThreadMode::<rocksdb::MultiThreaded>::open(&opts, dir);
            match db {
                Ok(db) => {
                    let kvs = Box::new(RocksDBKv { db: db });
                    Ok(kvs)
                }
                Err(err) => Err(KvError::IoError(err.to_string())),
            }
        }
    }
}

impl KvStore for SledKv {
    fn get(&self, key: &Vec<u8>) -> Result<Vec<u8>, KvError> {
        match self.db.get(key) {
            Ok(value) => match value {
                Some(value) => Ok(value.to_vec()),
                None => Err(KvError::NotFound("key not found".into())),
            },
            Err(err) => Err(KvError::IoError(err.to_string())),
        }
    }
    fn set(&self, key: &Vec<u8>, value: Vec<u8>) -> Result<(), KvError> {
        let r = self.db.insert(key, value);
        match r {
            Ok(_) => Ok(()),
            Err(err) => Err(KvError::IoError(err.to_string())),
        }
    }

    fn scan(
        &self,
        start: &Vec<u8>,
        end: &Vec<u8>,
        limit: u32,
        items: &mut Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<(), KvError> {
        let start = start.as_slice();
        let end = end.as_slice();
        let mut ct = 0 as u32;
        for sr in self.db.range(start..end) {
            match sr {
                Ok((key, value)) => {
                    items.push((key.to_vec(), value.to_vec()));
                    ct += 1;
                    if ct >= limit {
                        break;
                    }
                }
                Err(err) => {
                    return Err(KvError::IoError(err.to_string()));
                }
            }
        }
        Ok(())
    }

    fn remove(&self, key: &Vec<u8>) -> Result<(), KvError> {
        let r = self.db.remove(key);
        match r {
            Ok(_) => Ok(()),
            Err(err) => Err(KvError::IoError(err.to_string())),
        }
    }

    fn max_key(&self) -> Result<Vec<u8>, KvError> {
        let last = self.db.last();
        match last {
            Ok(last) => match last {
                Some((key, _)) => Ok(key.to_vec()),
                None => Err(KvError::NotFound("not found max key".into())),
            },
            Err(err) => Err(KvError::IoError(err.to_string())),
        }
    }
}

impl KvStore for RocksDBKv {
    fn get(&self, key: &Vec<u8>) -> Result<Vec<u8>, KvError> {
        match self.db.get(key.as_slice()) {
            Ok(value) => match value {
                Some(value) => Ok(value.to_vec()),
                None => Err(KvError::NotFound("key not found".into())),
            },
            Err(err) => Err(KvError::IoError(err.to_string())),
        }
    }
    fn set(&self, key: &Vec<u8>, value: Vec<u8>) -> Result<(), KvError> {
        let r = self.db.put(key, value);
        match r {
            Ok(_) => Ok(()),
            Err(err) => Err(KvError::IoError(err.to_string())),
        }
    }

    fn scan(
        &self,
        start: &Vec<u8>,
        end: &Vec<u8>,
        limit: u32,
        items: &mut Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<(), KvError> {
        let start = start.as_slice();
        let end = end.as_slice();
        let mut ct = 0 as u32;
        let it = self.db.iterator(rocksdb::IteratorMode::From(
            start,
            rocksdb::Direction::Forward,
        ));
        for (k, v) in it {
            let k = k.as_ref();
            let v = v.as_ref();
            if k.cmp(end) != Ordering::Less {
                break;
            }
            ct += 1;
            if ct > limit {
                break;
            }
            items.push((k.to_vec(), v.to_vec()));
        }
        Ok(())
    }

    fn remove(&self, key: &Vec<u8>) -> Result<(), KvError> {
        let r = self.db.delete(key.as_slice());
        match r {
            Ok(_) => Ok(()),
            Err(err) => Err(KvError::IoError(err.to_string())),
        }
    }

    fn max_key(&self) -> Result<Vec<u8>, KvError> {
        let it = self.db.iterator(rocksdb::IteratorMode::End);
        for (k, _v) in it {
            return Ok(k.as_ref().to_vec());
        }
        Err(KvError::NotFound("not found max key".into()))
    }
}

impl KvStore for Box<dyn KvStore> {
    fn get(&self, key: &Vec<u8>) -> Result<Vec<u8>, KvError> {
        self.as_ref().get(key)
    }
    fn set(&self, key: &Vec<u8>, value: Vec<u8>) -> Result<(), KvError> {
        self.as_ref().set(key, value)
    }
    fn scan(
        &self,
        start: &Vec<u8>,
        end: &Vec<u8>,
        limit: u32,
        items: &mut Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<(), KvError> {
        self.as_ref().scan(start, end, limit, items)
    }
    fn remove(&self, key: &Vec<u8>) -> Result<(), KvError> {
        self.as_ref().remove(key)
    }
    fn max_key(&self) -> Result<Vec<u8>, KvError> {
        self.as_ref().max_key()
    }
}
