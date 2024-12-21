use std::sync::Arc;
use db_common::{TableDefinition, ColumnDefinition, ColumnType};
use db_metadata::{RocksDBMetadataManager, MetadataManager};
use db_storage::S3StorageManager;
use db_worker::GPUWorker;
use db_coordinator::{Coordinator, SimpleCoordinator};
use log::{debug, error, info};
use tokio::sync::Mutex;
use actix_web::{web, App, HttpServer, Responder, HttpResponse};
use serde::{Serialize, Deserialize};
use dotenv::dotenv;

#[derive(Serialize, Deserialize)]
struct QueryRequest {
    query: String,
}

// Define a type alias for our coordinator to make it clearer
type AppCoordinator = Arc<dyn Coordinator + Send + Sync>;

async fn query(
    req_body: web::Json<QueryRequest>,
    coordinator: web::Data<AppCoordinator>
) -> impl Responder {
    let result = coordinator.execute_query(&req_body.query).await;
    info!("Executing query: {}", &req_body.query);
    debug!("{:?}", result);

    match result {
        Ok(mut rows) => {
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
    // Initialize logging
    env_logger::Builder::from_default_env()
        .filter_level(log::LevelFilter::Debug)
        .init();
    dotenv().ok();

    // Initialize components
    let db_path = "metadata.db";
    let metadata_manager = Arc::new(RocksDBMetadataManager::new(db_path)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?);
    
    let bucket_name = "columnar-test".to_string();
    let storage_manager = Arc::new(S3StorageManager::new(bucket_name, "iris.parquet".to_string()).await
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?);
    
    let gpu_worker = GPUWorker::new(storage_manager).await
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    let worker = Arc::new(Mutex::new(gpu_worker));

    // Create coordinator
    let coordinator: Arc<dyn Coordinator + Send + Sync> = Arc::new(
        SimpleCoordinator::new(metadata_manager, worker).await
    );

    // Create test table
    let table_def = TableDefinition {
        chunks: vec!["test.parquet".to_string()],
        name: "test".to_string(),
        columns: vec![
            ColumnDefinition {
                name: "id".to_string(),
                data_type: ColumnType::Integer
            },
            ColumnDefinition {
                name: "value".to_string(),
                data_type: ColumnType::String
            }
        ]
    };

    // Initialize table
    coordinator.create_table(&table_def).await
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

    info!("Starting HTTP server on 127.0.0.1:8080");

    // Start HTTP server
    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(coordinator.clone()))
            .route("/query", web::post().to(query))
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}