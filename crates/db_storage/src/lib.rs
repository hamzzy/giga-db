use db_common::{ColumnType, DbError, QueryResult, TableDefinition};
use async_trait::async_trait;
use parquet::file::{
    reader::{SerializedFileReader, FileReader},
    properties::ReaderProperties,
};
use parquet::record::RowAccessor;
use parquet::column::reader::{ColumnReader, get_typed_column_reader};
use aws_sdk_s3::Client;
use aws_config::{meta::region::RegionProviderChain, BehaviorVersion};
use bytes::Bytes;
use tokio::io::AsyncReadExt;
use futures::StreamExt;

#[async_trait]
pub trait StorageManager {
    async fn load_data_from_s3(&self, table_def: &TableDefinition, s3_key: &str) -> Result<QueryResult, DbError>;
    async fn load_column_f32(&self, table_def: &TableDefinition, s3_key: &str, column_index: usize) -> Result<Vec<f32>, DbError>;
}

#[derive(Debug, Clone)]
pub struct S3StorageManager {
    client: Client,
    bucket_name: String,
    s3_key : String
}

impl S3StorageManager {
    pub async fn new(bucket_name: String, s3_key:  String) -> Result<Self, DbError> {
        let region_provider = RegionProviderChain::default_provider().or_else("us-east-1");
        let config = aws_config::from_env().endpoint_url("http://127.0.0.1:9000").region(region_provider).load().await;
        let client = Client::new(&config);
        Ok(Self { client, bucket_name, s3_key })
    }

    async fn read_parquet_bytes(&self) -> Result<Bytes, DbError> {
        let resp = self.client.get_object()
            .bucket(&self.bucket_name)
            .key(&self.s3_key)
            .send()
            .await
            .map_err(|e| DbError::StorageError(e.to_string()))?;
        
        let body = resp.body;
        let mut bytes = Vec::with_capacity(resp.content_length.unwrap_or(1024) as usize);
        let mut stream = body.into_async_read();
        stream.read_to_end(&mut bytes)
            .await
            .map_err(|e| DbError::StorageError(e.to_string()))?;

        Ok(Bytes::from(bytes))
    }
}
#[async_trait]
impl StorageManager for S3StorageManager {
    async fn load_data_from_s3(&self, table_def: &TableDefinition, s3_key: &str) -> Result<QueryResult, DbError> {
        let bytes = self.read_parquet_bytes().await?;
        let reader = SerializedFileReader::new(bytes)
            .map_err(|e| DbError::StorageError(e.to_string()))?;

        let mut rows = Vec::new();
        let row_iter = reader.get_row_iter(None)
            .map_err(|e| DbError::StorageError(e.to_string()))?;

        // Pre-allocate based on file metadata
        let row_count = reader.metadata().file_metadata().num_rows() as usize;
        rows.reserve(row_count);

        for row_result in row_iter {
            let row = row_result.map_err(|e| DbError::StorageError(e.to_string()))?;
            let mut row_values = Vec::with_capacity(table_def.columns.len());

            for i in 0..table_def.columns.len() {
                let value = match table_def.columns[i].data_type {
                    ColumnType::Integer => row.get_int(i).map_err(|e| DbError::StorageError(e.to_string()))?.to_string(),
                    ColumnType::String => row.get_string(i).map_err(|e| DbError::StorageError(e.to_string()))?.to_string(),
                    ColumnType::Float => row.get_float(i).map_err(|e| DbError::StorageError(e.to_string()))?.to_string(),
                     ColumnType::Date => {
                        // Assuming dates are stored as strings or integers
                        let date_str = row.get_string(i).map_err(|e| DbError::StorageError(e.to_string()))?;
                        date_str.to_string()
                    },
                    ColumnType::Double => row.get_double(i).map_err(|e| DbError::StorageError(e.to_string()))?.to_string(),
                    // Handle other data types as needed
                    _ => "".to_string(),
                };
                row_values.push(value);
            }
            rows.push(row_values);
        }

        Ok(QueryResult { rows })
    }
    async fn load_column_f32(&self, table_def: &TableDefinition, s3_key: &str, column_index: usize) -> Result<Vec<f32>, DbError> {
        let bytes = self.read_parquet_bytes().await?;
        let reader = SerializedFileReader::new(bytes)
            .map_err(|e| DbError::StorageError(e.to_string()))?;
    
        let row_count = reader.metadata().file_metadata().num_rows() as usize;
        let mut column_data = Vec::with_capacity(row_count);
    
        let row_group_reader = reader.get_row_group(0)
            .map_err(|e| DbError::StorageError(e.to_string()))?;
    
        let column_reader = row_group_reader.get_column_reader(column_index)
            .map_err(|e| DbError::StorageError(e.to_string()))?;
    
        match column_reader {
            ColumnReader::FloatColumnReader(mut float_reader) => {
                let mut batch = vec![0.0; row_count];
                let mut def_levels = vec![0; row_count];
                let mut rep_levels = vec![0; row_count];
    
                let (_, num_values) = float_reader.read_batch(
                    row_count,
                    Some(&mut def_levels),
                    Some(&mut rep_levels),
                    &mut batch,
                )
                .map_err(|e| DbError::StorageError(e.to_string()))?;
    
                batch.truncate(num_values);
                column_data = batch;
            },
            ColumnReader::DoubleColumnReader(mut double_reader) => {
                let mut batch = vec![0.0; row_count];
                let mut def_levels = vec![0; row_count];
                let mut rep_levels = vec![0; row_count];
    
                let (_, num_values) = double_reader.read_batch(
                    row_count,
                    Some(&mut def_levels),
                    Some(&mut rep_levels),
                    &mut batch,
                )
                .map_err(|e| DbError::StorageError(e.to_string()))?;
    
                batch.truncate(num_values);
                column_data = batch.iter().map(|&x| x as f32).collect();
            },
            _ => {
                return Err(DbError::StorageError("Unsupported column type".to_string()));
            }
        }
    
        Ok(column_data)
    }
}