//! Live proof that `with_base_filter` clauses actually gate a scan through real OpenSearch — the
//! seam through which a caller injects an always-on row-security predicate. Self-contained: seeds
//! its own throwaway index, so it needs only a reachable OpenSearch (no app data). IGNORED by
//! default:
//!
//!   OS_URL=http://localhost:9200 cargo test -p datafusion-opensearch --test base_filter -- --ignored --nocapture

use std::sync::Arc;

use datafusion::arrow::array::{Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::SessionContext;
use datafusion_opensearch::OpenSearchTableProvider;
use serde_json::json;

#[tokio::test]
#[ignore = "needs a running OpenSearch (OS_URL)"]
async fn base_filter_gates_every_scan() {
    let url = std::env::var("OS_URL").unwrap_or_else(|_| "http://localhost:9200".into());
    let index = "datafusion-opensearch-basefilter-test";
    let http = reqwest::Client::new();

    // Fresh index with an explicit int mapping for the security field.
    let _ = http.delete(format!("{url}/{index}")).send().await.unwrap();
    http.put(format!("{url}/{index}"))
        .json(&json!({ "mappings": { "properties": { "id": { "type": "keyword" }, "level": { "type": "integer" } } } }))
        .send()
        .await
        .unwrap();
    // Four docs at levels 0/20/20/40.
    for (id, level) in [("a", 0), ("b", 20), ("c", 20), ("d", 40)] {
        http.put(format!("{url}/{index}/_doc/{id}?refresh=true"))
            .json(&json!({ "id": id, "level": level }))
            .send()
            .await
            .unwrap();
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, true),
        Field::new("level", DataType::Int64, true),
    ]));

    // A subject cleared to level 20: base_filter = level <= 20. This clause is applied to EVERY
    // scan and cannot be opted out of by the query.
    let provider = OpenSearchTableProvider::new(&url, index, schema)
        .with_base_filter(vec![json!({ "range": { "level": { "lte": 20 } } })]);
    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::new(provider)).unwrap();

    // Even an unfiltered SELECT * must not see the level-40 doc.
    let batches = ctx
        .sql("SELECT id, level FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut seen = vec![];
    for b in &batches {
        let ids = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let levels = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..b.num_rows() {
            assert!(
                levels.value(i) <= 20,
                "base_filter leaked a level-{} row",
                levels.value(i)
            );
            seen.push(ids.value(i).to_string());
        }
    }
    seen.sort();
    assert_eq!(seen, vec!["a", "b", "c"], "expected exactly the level<=20 docs");
    println!("[live] base_filter gated SELECT *: visible = {seen:?} (level-40 'd' correctly excluded)");

    // A user WHERE composes WITH the base_filter (AND), never replaces it: WHERE level = 40 returns
    // nothing, because base_filter already excluded it.
    let n: usize = ctx
        .sql("SELECT id FROM t WHERE level = 40")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .iter()
        .map(|b| b.num_rows())
        .sum();
    assert_eq!(n, 0, "base_filter must AND with the user predicate, not be overridable");
    println!("[live] WHERE level=40 → 0 rows (base_filter is not opt-out-able)");

    http.delete(format!("{url}/{index}")).send().await.unwrap();
}
