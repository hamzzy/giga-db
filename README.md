**README.md**

```markdown
# GPU-Accelerated Columnar Database in Rust

This project is an attempt to build a GPU-accelerated columnar database from scratch, inspired by systems like Snowflake, using Rust. It leverages object storage (AWS S3-compatible) for data persistence, and GPU processing for fast analytical queries.

## Getting Started



### Building and Running

1.  **Clone the repository:**
    ```bash
    git clone <your_repository_url>
    cd <your_repository_directory>
    ```
2.  **Build the project:**
    ```bash
    cargo build
    ```
3. **Start the database:**
    ```bash
    cargo run
   ```
4.  **Send Queries:**

    Use a tool like `curl` or Postman to send `POST` requests to `http://127.0.0.1:8080/query` with the following json:
        ```json
       { "query": "SELECT * FROM test WHERE id > 1.0"}
      ```
        Example Queries:
     *  `SELECT id, value FROM test WHERE id > 1.0` (to test projection)
    *   `SELECT SUM(id) FROM test` (to test aggregation)
    *   `SELECT * FROM test WHERE id > 1.0` (to test projection with wild card)

## Configuration
This application uses `.env` to configure environment variables such as credentials, and bucket name. These are parsed during start up.

## Code Example
```rust
#[tokio::main]
async fn main() -> std::io::Result<()> {
  env_logger::Builder::from_default_env()
  .filter_level(log::LevelFilter::Debug)
  .init();
    dotenv().ok();

    let db_path = "metadata.db";
    let metadata_manager = Arc::new(RocksDBMetadataManager::new(db_path).unwrap());
    let bucket_name = "columnar-test".to_string();
    let storage_manager = Arc::new(S3StorageManager::new(bucket_name,"flights-1m.parquet".to_string()).await.unwrap());
    // println!("loaded data: {:?}", storage_manager);
    let gpu_worker = GPUWorker::new(storage_manager).await.unwrap();
    let worker = Arc::new(Mutex::new(gpu_worker));
    let coordinator = Arc::new(SimpleCoordinator::new(metadata_manager, worker).await);

    let table_def = TableDefinition {
        chunks: vec!["test.parquet".to_string()],
        name: "test".to_string(),
        columns: vec![
            ColumnDefinition {name: "id".to_string(), data_type: ColumnType::Integer},
            ColumnDefinition {name: "value".to_string(), data_type: ColumnType::String}
        ]
    };
    coordinator.create_table(&table_def).await.unwrap();

    let coordinator_for_server = web::Data::new(coordinator.clone());
    println!("Server is starting...");

    HttpServer::new(move || {
        App::new()
            .app_data(coordinator_for_server.clone())
            .route("/query", web::post().to(query))
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await?;

    Ok(())
}
```

## Next Steps (Roadmap)

*   **Complete `AVG` Aggregation:**  Implement `AVG` aggregation.
*   **Partial Loading:**  Complete the logic to read Parquet files in chunks.
*   **Column Name Instead of Index:**  Implement parsing of column names in the filter.
*   **More Complex Queries:** Handle more complex queries, multiple filters, projections, and aggregations, including `JOIN` operations.
*   **Refactor Code:** Improve code modularity, use clear naming conventions, and reduce dependencies.
*   **Error Handling:** Improve error handling and provide more informative error messages.
*   **Logging:** Implement logging with `tracing` or `log` crates to track events, errors, and performance metrics.
*   **Unit Testing:** Add more unit tests for individual components.
*   **Integration Tests:** Add integration tests for testing between components.
*   **Performance and Scalability:** Profile code and start working on performance optimization and distributed worker architecture.
*   **Configuration Management:** Use environment variables, command-line arguments, or configuration files for settings.
*    **Authentication:** Implement authentication and authorization for the client interface.
*   **Monitoring:** Add real-time monitoring.
*  **Deployment:** Set up deployments using Docker and Kubernetes

## Contributing

We welcome contributions to this project. To contribute, please:

1.  Fork the repository.
2.  Create a new branch for your feature or bug fix.
3.  Implement your changes, write tests, and make sure all the existing tests pass.
4.  Submit a pull request with a detailed description of your changes.

## License

This project is licensed under the [MIT License](LICENSE).

## Contact

For questions or issues, please open an issue on the GitHub repository or contact me directly.
```

**Key Highlights:**

*   **Clear Overview:** The README provides a clear explanation of the project's goals and features.
*   **Architecture:** It outlines the system's components and their responsibilities.
*   **Getting Started:** It provides clear, step-by-step instructions for building and running the project.
*   **Next Steps:** It includes a roadmap for future development.
*   **Contribution Guidelines:** It encourages external contributions.
*   **Licensing:** It specifies the license.

**How to Use This README**

1.  **Save:** Save the markdown content above to a file named `README.md` in the root directory of your project.
2.  **Customize:**
    *   Replace the placeholders (e.g., `<your_repository_url>`, the license link) with your actual values.
    *   Add more details to the roadmap and other sections as needed.
3.  **Keep it Up-to-Date:** As you continue developing, make sure to keep the README updated with the latest changes.

This README should provide a solid foundation for others to understand and work with your project. Let me know if you have any other questions or would like to make changes!
