use std::sync::Arc;
use db_common::{TableDefinition, ColumnDefinition, ColumnType};
use db_metadata::{RocksDBMetadataManager, MetadataManager};
use db_storage::{S3StorageManager, StorageManager};
use db_worker::{GPUWorker, Worker};
use db_coordinator::{Coordinator, SimpleCoordinator};
use db_client::DbClient;
use aws_sdk_s3::Region;
use tokio::sync::Mutex;
use actix_web::{web, App, HttpServer, Responder, HttpResponse};
use serde::{Serialize, Deserialize};
#[derive(Serialize, Deserialize)]
struct QueryRequest {
    query: String,
}

async fn query(
    req_body: web::Json<QueryRequest>, 
    coordinator: web::Data<Arc<dyn Coordinator + Send + Sync>>
) -> impl Responder {
   let result = coordinator.execute_query(&req_body.query).await;
   match result {
        Ok(result) => HttpResponse::Ok().json(result),
        Err(error) => HttpResponse::InternalServerError().body(error.to_string())
   }
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
  let db_path = "metadata.db";
  let metadata_manager = Arc::new(RocksDBMetadataManager::new(db_path).unwrap());
  let local_storage_path = "local_storage";
  let bucket_name = "my_s3_bucket".to_string();
  let region = Region::from_static("us-east-1");
  let storage_manager = Arc::new(S3StorageManager::new(bucket_name, local_storage_path.to_string(), region).await.unwrap());
  let gpu_worker = GPUWorker::new(storage_manager).await.unwrap();
  let worker = Arc::new(Mutex::new(gpu_worker));
  let coordinator = Arc::new(SimpleCoordinator::new(metadata_manager, worker).await);

  let table_def = TableDefinition {
     name: "test".to_string(),
        columns: vec![
            ColumnDefinition {name: "id".to_string(), data_type: ColumnType::Integer},
            ColumnDefinition {name: "value".to_string(), data_type: ColumnType::String}
        ]
    };
    coordinator.create_table(&table_def).await.unwrap();
     // Create data in S3
    let data = db_common::QueryResult {
        rows: vec![
            vec!["1".to_string(), "test".to_string()],
            vec!["2".to_string(), "another".to_string()],
            vec!["3".to_string(), "another2".to_string()],
             vec!["4".to_string(), "another3".to_string()],
        ]
    };
   let s3_key = "test.parquet";
   let s3_storage_manager = Arc::clone(&coordinator).downcast::<SimpleCoordinator<RocksDBMetadataManager, GPUWorker>>().unwrap();
    let s3_storage = s3_storage_manager.metadata_manager.clone();
    let downcasted_s3_manager = Arc::clone(&s3_storage_manager).downcast::<SimpleCoordinator<RocksDBMetadataManager, GPUWorker>>().unwrap();
    let storage = downcasted_s3_manager.worker.lock().await.storage_manager.clone();
    // We no longer write to parquet file
    // storage.write_data_to_s3(&table_def, data, s3_key).await.unwrap();

    
  let coordinator_for_server = Arc::clone(&coordinator);
    HttpServer::new(move || {
         App::new()
            .app_data(web::Data::new(coordinator_for_server.clone()))
            .route("/query", web::post().to(query))
      })
        .bind(("127.0.0.1", 8080))?
         .run()
         .await?;


    Ok(())
}