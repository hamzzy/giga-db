use db_common::{DbError, QueryResult};
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
struct QueryRequest {
    query: String,
}

pub struct DbClient {
    server_address: String,
}

impl DbClient {
    pub fn new(server_address: String) -> Self {
        Self { server_address }
    }

    pub async fn execute_query(&self, query: &str) -> Result<QueryResult, DbError> {
       let client = reqwest::Client::new();
       let request_body = QueryRequest{query: query.to_string()};
        let response = client
            .post(format!("{}/query", self.server_address))
            .json(&request_body)
            .send()
            .await
            .map_err(|e| DbError::ClientError(e.to_string()))?;
        
       if response.status().is_success() {
           let result = response.json::<QueryResult>().await
            .map_err(|e| DbError::ClientError(e.to_string()))?;
          Ok(result)
       } else {
         Err(DbError::ClientError(response.status().to_string()))
       }
    }
}