use aws_sdk_s3::{Client};
use aws_config::meta::region::RegionProviderChain;
use anyhow::Result;

pub async fn stream_data_from_s3(bucket: &str, key: &str) -> Result<String> {
    let region_provider = RegionProviderChain::default_provider().or_else("us-east-1");
    let config = aws_config::from_env().endpoint_url("http://198.19.249.3:9000/").region(region_provider).load().await;
    let s3 = Client::new(&config);
    let result = s3.get_object().key(key).bucket(bucket).send().await?;
    let body = result.body.collect().await.expect("reading body succeeds").into_bytes();
    Ok(String::from_utf8_lossy(&body).to_string())
}


