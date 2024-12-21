use db_common::{DbError, TableDefinition, ColumnDefinition};
use async_trait::async_trait;
use rocksdb::{Options, DB};
use serde_json;

#[async_trait]
pub trait MetadataManager {
   async fn create_table(&self, table_def: &TableDefinition) -> Result<(), DbError>;
   async fn get_table(&self, table_name: &str) -> Result<Option<TableDefinition>, DbError>;
    async fn get_chunks(&self, table_name: &str) -> Result<Vec<String>, DbError>;
}

pub struct RocksDBMetadataManager {
    db: DB,
}

impl RocksDBMetadataManager {
     pub fn new(db_path: &str) -> Result<Self, DbError> {
        let mut options = Options::default();
        options.create_if_missing(true);
        let db = DB::open(&options, db_path)
             .map_err(|e| DbError::MetadataError(e.to_string()))?;
        Ok(Self{db})
    }
}


#[async_trait]
impl MetadataManager for RocksDBMetadataManager {
    async fn create_table(&self, table_def: &TableDefinition) -> Result<(), DbError> {
        let key = table_def.name.clone();
        let serialized_value = serde_json::to_string(table_def)
             .map_err(|e| DbError::MetadataError(e.to_string()))?;
        self.db.put(key.as_bytes(), serialized_value.as_bytes())
             .map_err(|e| DbError::MetadataError(e.to_string()))?;
        Ok(())
    }

    async fn get_table(&self, table_name: &str) -> Result<Option<TableDefinition>, DbError> {
        let key = table_name.clone();
       let result = self.db.get(key.as_bytes())
             .map_err(|e| DbError::MetadataError(e.to_string()))?;

        match result {
            Some(value) => {
                let serialized_value = String::from_utf8(value)
                     .map_err(|e| DbError::MetadataError(e.to_string()))?;
                let table_def: TableDefinition = serde_json::from_str(&serialized_value)
                     .map_err(|e| DbError::MetadataError(e.to_string()))?;
                Ok(Some(table_def))
            },
            None => Ok(None)
        }
    }

    async fn get_chunks(&self, table_name: &str) -> Result<Vec<String>, DbError> {
       let table = self.get_table(table_name).await?;
         match table {
            Some(table) => Ok(table.chunks),
            None =>  Ok(Vec::new()),
        }
   }
}