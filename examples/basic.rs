//! Register an OpenSearch index as a DataFusion table (schema inferred from `_mapping`) and run a
//! query with filter / projection / limit pushdown, streaming the result.
//!
//!   OS_URL=http://localhost:9200 OS_INDEX=my-index cargo run --example basic

use datafusion::prelude::SessionContext;
use datafusion_opensearch::{OpenSearchClient, OpenSearchTableFactory};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("OS_URL").unwrap_or_else(|_| "http://localhost:9200".into());
    let index = std::env::var("OS_INDEX").unwrap_or_else(|_| "my-index".into());

    let factory = OpenSearchTableFactory::new(OpenSearchClient::new(url));
    let table = factory.table_provider(&index).await?; // Arrow schema derived from the index mapping
    println!(
        "{index}: {} columns inferred from _mapping",
        table.schema().fields().len()
    );

    let ctx = SessionContext::new();
    ctx.register_table("docs", table)?;
    // The WHERE becomes a `bool.filter`, the projection `_source`, the LIMIT the page size.
    ctx.sql("SELECT * FROM docs LIMIT 10").await?.show().await?;
    // EXPLAIN VERBOSE shows the exact `_search` body the scan sends.
    ctx.sql("EXPLAIN VERBOSE SELECT * FROM docs LIMIT 10")
        .await?
        .show()
        .await?;
    Ok(())
}
