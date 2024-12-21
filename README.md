
# GPU-Accelerated Columnar Database in Rust

This project is an attempt to build a GPU-accelerated columnar database from scratch, inspired by systems like Snowflake, using Rust. It leverages object storage (AWS S3-compatible) for data persistence, and GPU processing for fast analytical queries.

## Getting Started



### Building and Running

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/hamzzy/giga-db
    cd giga-db
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


## Contributing

We welcome contributions to this project. To contribute, please:

1.  Fork the repository.
2.  Create a new branch for your feature or bug fix.
3.  Implement your changes, write tests, and make sure all the existing tests pass.
4.  Submit a pull request with a detailed description of your changes.


