use strum_macros::Display;
use tracing::info;

pub enum DbKind {
    SLED,
    LEVELDB,
}

#[derive(Debug, Display)]
pub enum KvError {
    NotFound(String),
    IoError(String),
    Unknown(&'static str),
}

pub trait KvStore: Send + Sync + 'static {
    fn get(&self, key: &Vec<u8>) -> Result<Vec<u8>, KvError>;
    fn set(&self, key: &Vec<u8>, value: Vec<u8>) -> Result<(), KvError>;
    fn min_key(&self) -> Result<Vec<u8>, KvError>;
    fn max_key(&self) -> Result<Vec<u8>, KvError>;
    fn scan(
        &self,
        start: &Vec<u8>,
        end: &Vec<u8>,
        limit: u32,
        items: &mut Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<(), KvError>;
    fn remove(&self, key: &Vec<u8>) -> Result<(), KvError>;
}

#[derive(Debug)]
struct SledKv {
    db: sled::Db,
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
        DbKind::LEVELDB => Err(KvError::Unknown("unimplemented db, on todo list".into())),
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

    fn min_key(&self) -> Result<Vec<u8>, KvError> {
        let first = self.db.first();
        match first {
            Ok(first) => match first {
                Some((key, _)) => Ok(key.to_vec()),
                None => Err(KvError::NotFound("not found min key".into())),
            },
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

    fn remove(&self, key: &Vec<u8>) -> Result<(), KvError> {
        let r = self.db.remove(key);
        match r {
            Ok(_) => Ok(()),
            Err(err) => Err(KvError::IoError(err.to_string())),
        }
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
    fn min_key(&self) -> Result<Vec<u8>, KvError> {
        self.as_ref().min_key()
    }
    fn max_key(&self) -> Result<Vec<u8>, KvError> {
        self.as_ref().max_key()
    }
    fn remove(&self, key: &Vec<u8>) -> Result<(), KvError> {
        self.as_ref().remove(key)
    }
}
