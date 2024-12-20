use thiserror::Error;
use serde::{Serialize, Deserialize};

#[derive(Error, Debug)]
pub enum DbError {
    #[error("General database error: {0}")]
    General(String),
    #[error("SQL parsing error: {0}")]
    SqlParsingError(String),
    #[error("Metadata error: {0}")]
    MetadataError(String),
    #[error("Storage error: {0}")]
    StorageError(String),
    #[error("Worker error: {0}")]
    WorkerError(String),
    #[error("Coordinator error: {0}")]
    CoordinatorError(String),
    #[error("Client error: {0}")]
    ClientError(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDefinition {
    pub name: String,
    pub data_type: ColumnType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ColumnType {
    Integer,
    Float,
    String,
    Boolean,
    // Add other types as needed
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableDefinition {
    pub name: String,
    pub columns: Vec<ColumnDefinition>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    pub rows: Vec<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Filter {
    pub column_index: usize,
    pub value: String,
    pub operator: FilterOperator
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FilterOperator {
    GreaterThan,
    LessThan,
    Equal,
    NotEqual,
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataPartition {
     pub column_name: String,
     pub value: String
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableChunk {
   pub s3_key: String,
   pub data_partition: Option<DataPartition>,
   pub worker_id: Option<u64>
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryPlan {
    pub chunks: Vec<TableChunk>,
    pub filter: Option<Filter>,
    pub column_name: Option<String>,
}

#[derive(Debug, Clone)]
pub enum ExecutionTree {
    Select { plan: QueryPlan},
}