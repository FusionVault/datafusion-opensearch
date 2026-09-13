//! SQL only: `CREATE EXTERNAL TABLE … STORED AS OPENSEARCH`, plus the whole cluster as a schema.
//!
//!   OS_URL=http://localhost:9200 OS_INDEX=my-index cargo run --example sql

use std::sync::Arc;

use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_opensearch::{OpenSearchClient, OpenSearchSchemaProvider, OpenSearchTableProviderFactory};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("OS_URL").unwrap_or_else(|_| "http://localhost:9200".into());
    let index = std::env::var("OS_INDEX").unwrap_or_else(|_| "my-index".into());
    // information_schema on, so SHOW TABLES lists the cluster's indices.
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_information_schema(true));

    // One index, from SQL.
    OpenSearchTableProviderFactory::new().register(&ctx);
    ctx.sql(&format!(
        "CREATE EXTERNAL TABLE docs STORED AS OPENSEARCH LOCATION '{url}/{index}'"
    ))
    .await?
    .collect()
    .await?;
    ctx.sql("SELECT count(*) FROM docs").await?.show().await?;

    // Every index, as the `os` schema.
    let os = OpenSearchSchemaProvider::connect(OpenSearchClient::new(&url)).await?;
    ctx.catalog("datafusion").unwrap().register_schema("os", Arc::new(os))?;
    ctx.sql("SHOW TABLES").await?.show().await?;
    Ok(())
}
