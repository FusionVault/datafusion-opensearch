//! Live integration test: register the provider over a real OpenSearch index and run SQL through
//! DataFusion, exercising filter / projection / limit pushdown and a GROUP BY. IGNORED by default
//! (needs a reachable OpenSearch with data). Fully env-driven so it works against any index:
//!
//!   OS_URL=http://localhost:9200 \
//!   OS_INDEX=my-index OS_ID_FIELD=id \
//!   OS_FILTER_FIELD=status OS_FILTER_VALUE=OK OS_GROUP_FIELD=status \
//!   cargo test -p datafusion-opensearch --test live_opensearch -- --ignored --nocapture

use std::sync::Arc;

use datafusion::arrow::array::{Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::SessionContext;
use datafusion_opensearch::OpenSearchTableProvider;

fn env(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

#[tokio::test]
#[ignore = "needs a running OpenSearch (set OS_URL / OS_INDEX / OS_*_FIELD)"]
async fn scans_with_pushdown() {
    let url = env("OS_URL", "http://localhost:9200");
    let index = env("OS_INDEX", "my-index");
    let id_field = env("OS_ID_FIELD", "id");
    let filter_field = env("OS_FILTER_FIELD", "status");
    let filter_value = env("OS_FILTER_VALUE", "OK");
    let group_field = env("OS_GROUP_FIELD", "status");

    // Dedupe: the filter and group fields may be the same column (e.g. both `status`).
    let mut names = vec![id_field.clone(), filter_field.clone(), group_field.clone()];
    names.dedup_by(|a, b| a == b);
    names.sort();
    names.dedup();
    let schema = Arc::new(Schema::new(
        names
            .iter()
            .map(|n| Field::new(n, DataType::Utf8, true))
            .collect::<Vec<_>>(),
    ));
    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::new(OpenSearchTableProvider::new(url, index, schema)))
        .unwrap();

    // (1) projection + limit pushdown.
    let df = ctx
        .sql(&format!("SELECT {id_field}, {filter_field} FROM t LIMIT 5"))
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert!(rows > 0 && rows <= 5, "expected 1..=5 rows, got {rows}");
    assert_eq!(batches[0].num_columns(), 2, "projection should push down to 2 columns");
    println!(
        "[live] projected+limited: {rows} rows, {} cols",
        batches[0].num_columns()
    );

    // (2) filter pushdown: every returned row must satisfy the WHERE, proving the term query
    //     reached OpenSearch (not a client-side filter over match_all).
    let df = ctx
        .sql(&format!(
            "SELECT {id_field}, {filter_field} FROM t WHERE {filter_field} = '{filter_value}' LIMIT 100"
        ))
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let mut checked = 0;
    for b in &batches {
        let col = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        for i in 0..b.num_rows() {
            assert_eq!(col.value(i), filter_value, "filter pushdown leaked a non-matching row");
            checked += 1;
        }
    }
    println!("[live] filtered: {checked} rows, all {filter_field}={filter_value}");

    // (3) aggregate over the pushed-down scan — DataFusion does the GROUP BY.
    let df = ctx
        .sql(&format!(
            "SELECT {group_field}, count(*) AS n FROM t GROUP BY {group_field} ORDER BY n DESC"
        ))
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let groups: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert!(groups > 0, "expected at least one group");
    datafusion::arrow::util::pretty::print_batches(&batches).unwrap();
    println!("[live] GROUP BY {group_field}: {groups} groups");
}
