use db_common::{DbError, TableDefinition, QueryResult, ExecutionTree, Filter, QueryPlan};
use db_metadata::MetadataManager;
use db_sql::{SqlParser, execute};
use db_worker::Worker;
use std::sync::Arc;
use async_trait::async_trait;
use tokio::sync::Mutex;

#[async_trait]
pub trait Coordinator {
    async fn create_table(&self, table_def: &TableDefinition) -> Result<(), DbError>;
    async fn execute_query(&self, query: &str) -> Result<QueryResult, DbError>;
}

pub struct SimpleCoordinator<M: MetadataManager, W: Worker> {
    metadata_manager: Arc<M>,
    sql_parser: SqlParser,
    worker: Arc<Mutex<W>>
}


impl<M: MetadataManager, W: Worker> SimpleCoordinator<M, W> {
    pub async fn new(metadata_manager: Arc<M>, worker: Arc<Mutex<W>>) -> Self {
        Self {
            metadata_manager,
            sql_parser: SqlParser,
            worker
        }
    }
}

#[async_trait]
impl<M: MetadataManager + Send + Sync + 'static, W: Worker + Send + Sync + 'static> Coordinator for SimpleCoordinator<M, W> {
    async fn create_table(&self, table_def: &TableDefinition) -> Result<(), DbError> {
        self.metadata_manager.create_table(table_def).await
    }

   async fn execute_query(&self, query: &str) -> Result<QueryResult, DbError> {
       let parsed_sql = SqlParser::parse_sql(query)?;
       let mut execution_tree = SqlParser::transform_sql(parsed_sql)?;
      if let ExecutionTree::Select { plan } = &mut execution_tree {
           let chunks = self.metadata_manager.get_chunks("test").await?;
            plan.chunks = chunks;
           
           if let Some(column_name) = &plan.column_name {
                let table = self.metadata_manager.get_table("test").await?.unwrap();
                 let index = table.columns.iter().position(|c| &c.name == column_name)
                     .ok_or(DbError::CoordinatorError("Column name not found".to_string()))?;
                    if let Some(filter) = &mut plan.filter {
                        filter.column_index = index;
                   }
            }
       }

       // Lock the worker and execute the query on it
        let worker = self.worker.lock().await;
        let result = worker.execute_query_fragment(&execution_tree).await?;
        Ok(result)
    }
}