use db_common::{DbError, TableDefinition, QueryResult};
use async_trait::async_trait;
use parquet::file::{reader::FileReader};
use parquet::record::{RowAccessor};
use std::{fs::File};
use aws_sdk_s3::{Client};
use aws_config::BehaviorVersion;

#[async_trait]
pub trait StorageManager {
    async fn load_data_from_s3(&self, table_def: &TableDefinition, s3_key: &str) -> Result<QueryResult, DbError>;
    async fn load_column_f32(&self, table_def: &TableDefinition, s3_key: &str, column_index: usize) -> Result<Vec<f32>, DbError>;
}

pub struct S3StorageManager {
    client: Client,
    bucket_name: String,
    local_storage_path: String,
}

impl S3StorageManager {
    pub async fn new(bucket_name: String, local_storage_path: String) -> Result<Self, DbError> {
        let config = aws_config::load_from_env().await;
        let client = Client::new(&config);
        Ok(Self { client, bucket_name, local_storage_path })
    }
}

#[async_trait]
impl StorageManager for S3StorageManager {
    async fn load_data_from_s3(&self, table_def: &TableDefinition, s3_key: &str) -> Result<QueryResult, DbError> {
        let resp = self.client.get_object()
            .bucket(&self.bucket_name)
            .key(s3_key)
            .send()
            .await
            .map_err(|e| DbError::StorageError(e.to_string()))?;

        let body = resp.body;
        let bytes = body.collect()
            .await
            .map_err(|e| DbError::StorageError(e.to_string()))?
            .into_bytes();

        let path = format!("{}/{}", self.local_storage_path, s3_key);
        tokio::fs::write(&path, &bytes)
            .await
            .map_err(|e| DbError::StorageError(e.to_string()))?;

        // Read local Parquet
        let file = File::open(&path)
            .map_err(|e| DbError::StorageError(e.to_string()))?;
        let reader = parquet::file::reader::SerializedFileReader::new(file)
            .map_err(|e| DbError::StorageError(e.to_string()))?;

        let mut rows = Vec::new();
        for row_result in reader {
            let row = row_result.map_err(|e| DbError::StorageError(e.to_string()))?;
            let mut row_values = Vec::new();
            for i in 0..table_def.columns.len() {
                let value = row.get_string(i)
                    .map_err(|e| DbError::StorageError(e.to_string()))?
                    .to_string();
                row_values.push(value);
            }
            rows.push(row_values);
        }
        Ok(QueryResult { rows })
    }

    async fn load_column_f32(&self, table_def: &TableDefinition, s3_key: &str, column_index: usize) -> Result<Vec<f32>, DbError> {
        let resp = self.client.get_object()
            .bucket(&self.bucket_name)
            .key(s3_key)
            .send()
            .await
            .map_err(|e| DbError::StorageError(e.to_string()))?;

        let body = resp.body;
        let bytes = body.collect()
            .await
            .map_err(|e| DbError::StorageError(e.to_string()))?
            .into_bytes();

        let path = format!("{}/{}", self.local_storage_path, s3_key);
        tokio::fs::write(&path, &bytes)
            .await
            .map_err(|e| DbError::StorageError(e.to_string()))?;

        // Read local Parquet
        let file = File::open(&path)
            .map_err(|e| DbError::StorageError(e.to_string()))?;
        let reader = parquet::file::reader::SerializedFileReader::new(file)
            .map_err(|e| DbError::StorageError(e.to_string()))?;

        let mut column_data = Vec::new();
        for row_result in reader {
            let row = row_result.map_err(|e| DbError::StorageError(e.to_string()))?;
            let value = row.get_string(column_index)
                .map_err(|e| DbError::StorageError(e.to_string()))?;
            let parsed_value = value.parse::<f32>()
                .map_err(|e| DbError::StorageError(e.to_string()))?;
            column_data.push(parsed_value);
        }
        Ok(column_data)
    }
}