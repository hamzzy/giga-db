mod s3_utils;
use dotenv::dotenv;


#[tokio::main]
async fn main() {
    dotenv().ok();

    match s3_utils::stream_data_from_s3("columnar-test", "flights-1m.parquet").await {
        Ok(data) => println!("Data from S3: {:#?}", data),
        Err(e) => eprintln!("Error streaming data from S3: {:?}", e),
    }
}