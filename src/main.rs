use std::sync::Arc;
use db_common::{TableDefinition, ColumnDefinition, ColumnType};
use db_metadata::{RocksDBMetadataManager, MetadataManager};
use db_storage::{S3StorageManager, StorageManager};
use db_worker::{GPUWorker, Worker};
use db_coordinator::{Coordinator, SimpleCoordinator};
use db_client::DbClient;
use log::{info, error};
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
      Ok(mut rows) => {
          // Sorting logic here (e.g., sort by the first column)
          rows.rows.sort_by(|a, b| a[0].cmp(&b[0]));
          info!("Query executed successfully and sorted.");
          HttpResponse::Ok().json(rows)
      },
      Err(error) => {
          error!("Error executing query: {}", error);
          HttpResponse::InternalServerError().body(error.to_string())
      }
  }
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
  env_logger::init(); // Initialize logging

  let db_path = "metadata.db";
  let metadata_manager = Arc::new(RocksDBMetadataManager::new(db_path).unwrap());
  let local_storage_path = "local_storage";
  let bucket_name = "columnar-test".to_string();
  let storage_manager = Arc::new(S3StorageManager::new(bucket_name, local_storage_path.to_string()).await.unwrap());
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


  let coordinator_for_server = Arc::clone(&coordinator);
    HttpServer::new(move || {
         App::new()
            .app_data(web::Data::new(coordinator_for_server.clone()))
            .route("/query", web::post().to(query))
      })
        .bind(("127.0.0.1", 8080))?
         .run()
         .await?;
        println!("Server is starting...");


    Ok(())
}