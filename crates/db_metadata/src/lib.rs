use db_common::{DbError, TableDefinition, ColumnDefinition, TableChunk};
use async_trait::async_trait;
use rocksdb::{Options, DB};
use serde::{Serialize, Deserialize};

#[async_trait]
pub trait MetadataManager {
   async fn create_table(&self, table_def: &TableDefinition) -> Result<(), DbError>;
   async fn get_table(&self, table_name: &str) -> Result<Option<TableDefinition>, DbError>;
   async fn create_chunk(&self, table_name: &str, table_chunk: &TableChunk) -> Result<(), DbError>;
   async fn get_chunks(&self, table_name: &str) -> Result<Vec<TableChunk>, DbError>;

}

pub struct RocksDBMetadataManager {
    db: DB,
}

impl RocksDBMetadataManager {
    pub fn new(db_path: &str) -> Result<Self, DbError> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = DB::open(&opts, db_path)
            .map_err(|e| DbError::MetadataError(e.to_string()))?;
        Ok(Self { db })
    }

     fn table_key(table_name: &str) -> Vec<u8> {
        format!("table:{}", table_name).into_bytes()
    }

    fn chunk_key(table_name: &str, s3_key: &str) -> Vec<u8> {
        format!("chunk:{}:{}", table_name, s3_key).into_bytes()
    }
}


#[async_trait]
impl MetadataManager for RocksDBMetadataManager {
     async fn create_table(&self, table_def: &TableDefinition) -> Result<(), DbError> {
       let key = Self::table_key(&table_def.name);
        let value = serde_json::to_vec(table_def)
           .map_err(|e| DbError::MetadataError(e.to_string()))?;
       self.db.put(&key, &value).map_err(|e| DbError::MetadataError(e.to_string()))?;
       Ok(())
    }

    async fn get_table(&self, table_name: &str) -> Result<Option<TableDefinition>, DbError> {
         let key = Self::table_key(table_name);
        match self.db.get(&key).map_err(|e| DbError::MetadataError(e.to_string()))? {
            Some(value) => {
                let table_def: TableDefinition = serde_json::from_slice(&value)
                    .map_err(|e| DbError::MetadataError(e.to_string()))?;
                Ok(Some(table_def))
            },
            None => Ok(None)
        }
    }
    async fn create_chunk(&self, table_name: &str, table_chunk: &TableChunk) -> Result<(), DbError> {
        let key = Self::chunk_key(table_name, &table_chunk.s3_key);
        let value = serde_json::to_vec(table_chunk)
           .map_err(|e| DbError::MetadataError(e.to_string()))?;
       self.db.put(&key, &value).map_err(|e| DbError::MetadataError(e.to_string()))?;
       Ok(())
    }

     async fn get_chunks(&self, table_name: &str) -> Result<Vec<TableChunk>, DbError> {
       let mut chunks = Vec::new();
         let prefix = format!("chunk:{}:", table_name).into_bytes();
        let iter = self.db.prefix_iterator(&prefix);
         for item in iter {
             let (key, value) = item
             .map_err(|e| DbError::MetadataError(e.to_string()))?;
          let chunk: TableChunk = serde_json::from_slice(&value)
            .map_err(|e| DbError::MetadataError(e.to_string()))?;
           chunks.push(chunk)
         }

      Ok(chunks)
    }
}