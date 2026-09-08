//! End-to-end proof against a REAL OpenSearch started by testcontainers: schema inference from
//! `_mapping`, filter/projection/limit pushdown, the always-on base filter, and (with the `udf`
//! feature) the native `match` and geo pushdowns. Self-contained — it creates and drops its own
//! index. Needs Docker, so it is ignored by default:
//!
//!   cargo test -p datafusion-opensearch --all-features --test testcontainers -- --ignored --nocapture
//!
//! The container mirrors a typical local single-node setup (security plugin disabled).

use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::array::{Array, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::prelude::SessionContext;
use datafusion_opensearch::{OpenSearchClient, OpenSearchTableFactory};
use serde_json::json;
use testcontainers::core::IntoContainerPort;
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

const IMAGE: &str = "opensearchproject/opensearch";
const TAG: &str = "2.17.1";
const INDEX: &str = "datafusion-opensearch-it";
/// OpenSearch takes a while to come up; poll the health endpoint rather than rely on log lines.
const STARTUP: Duration = Duration::from_secs(180);

/// Wait until `/_cluster/health` answers 200 (yellow or green), or give up after [`STARTUP`].
async fn wait_healthy(http: &reqwest::Client, url: &str) {
    let deadline = std::time::Instant::now() + STARTUP;
    loop {
        if let Ok(r) = http.get(format!("{url}/_cluster/health")).send().await {
            if r.status().is_success() {
                return;
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "OpenSearch did not become healthy within {STARTUP:?}"
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

/// Collect one Utf8 column across all batches, sorted.
fn strings(batches: &[datafusion::arrow::record_batch::RecordBatch], col: usize) -> Vec<String> {
    let mut out = vec![];
    for b in batches {
        let a = b.column(col).as_any().downcast_ref::<StringArray>().unwrap();
        out.extend((0..a.len()).filter(|&i| !a.is_null(i)).map(|i| a.value(i).to_string()));
    }
    out.sort();
    out
}

#[tokio::test]
#[ignore = "needs Docker (testcontainers)"]
async fn end_to_end_against_a_real_opensearch() {
    let container = GenericImage::new(IMAGE, TAG)
        .with_exposed_port(9200.tcp())
        .with_env_var("discovery.type", "single-node")
        .with_env_var("DISABLE_SECURITY_PLUGIN", "true")
        .with_env_var("DISABLE_INSTALL_DEMO_CONFIG", "true")
        .with_env_var("cluster.routing.allocation.disk.threshold_enabled", "false")
        .with_env_var("OPENSEARCH_JAVA_OPTS", "-Xms512m -Xmx512m")
        .start()
        .await
        .expect("start opensearch container");
    let port = container.get_host_port_ipv4(9200).await.unwrap();
    let url = format!("http://127.0.0.1:{port}");
    let http = reqwest::Client::new();
    wait_healthy(&http, &url).await;

    // ── seed: an index with an explicit mapping covering every type family we read ──
    http.put(format!("{url}/{INDEX}"))
        .json(&json!({ "mappings": { "properties": {
            "id": { "type": "keyword" },
            "status": { "type": "keyword" },
            "speed": { "type": "double" },
            "count": { "type": "long" },
            "active": { "type": "boolean" },
            "title": { "type": "text" },
            "pos": { "type": "geo_point" },
        } } }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    let docs = [
        json!({ "id": "a", "status": "OK",   "speed": 55.0, "count": 1, "active": true,  "title": "engine fault cleared",  "pos": { "lat": 60.17, "lon": 24.94 } }), // Helsinki
        json!({ "id": "b", "status": "OK",   "speed": 30.0, "count": 2, "active": false, "title": "routine service",        "pos": { "lat": 59.33, "lon": 18.07 } }), // Stockholm
        json!({ "id": "c", "status": "DOWN", "speed": 70.0, "count": 3, "active": true,  "title": "engine overheating",     "pos": { "lat": 55.68, "lon": 12.57 } }), // Copenhagen
        json!({ "id": "d", "status": "OK",   "speed": 45.0, "count": 4, "active": true,  "title": "brakes worn",            "pos": { "lat": 59.91, "lon": 10.75 } }), // Oslo
    ];
    for d in &docs {
        http.put(format!("{url}/{INDEX}/_doc/{}?refresh=true", d["id"].as_str().unwrap()))
            .json(d)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
    }

    // ── 1) schema inferred from _mapping ──
    let factory = OpenSearchTableFactory::new(OpenSearchClient::new(&url));
    let table = factory.table_provider(INDEX).await.expect("schema from _mapping");
    let schema = table.schema();
    let typed: Vec<(String, DataType)> = schema
        .fields()
        .iter()
        .map(|f| (f.name().clone(), f.data_type().clone()))
        .collect();
    assert_eq!(
        typed,
        vec![
            ("active".into(), DataType::Boolean),
            ("count".into(), DataType::Int64),
            ("id".into(), DataType::Utf8),
            ("pos".into(), DataType::Utf8), // geo_point → JSON text
            ("speed".into(), DataType::Float64),
            ("status".into(), DataType::Utf8),
            ("title".into(), DataType::Utf8),
        ]
    );
    let ctx = SessionContext::new();
    ctx.register_table("t", table).unwrap();

    // ── 2) filter + projection + limit pushdown ──
    let batches = ctx
        .sql("SELECT id FROM t WHERE status = 'OK' AND speed > 40")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(strings(&batches, 0), vec!["a", "d"]);
    assert_eq!(batches[0].num_columns(), 1, "projection pushed to one column");
    let n: usize = ctx
        .sql("SELECT id FROM t LIMIT 2")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .iter()
        .map(|b| b.num_rows())
        .sum();
    assert_eq!(n, 2, "LIMIT pushed into size");
    let batches = ctx
        .sql("SELECT id FROM t WHERE count BETWEEN 2 AND 3 AND active")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(strings(&batches, 0), vec!["c"]);

    // ── 3) the always-on base filter composes with (never replaced by) the user's WHERE ──
    let gated = factory
        .provider_with_schema(INDEX, schema.clone())
        .with_base_filter(vec![json!({ "term": { "status": "OK" } })]);
    let ctx2 = SessionContext::new();
    ctx2.register_table("g", Arc::new(gated)).unwrap();
    let batches = ctx2.sql("SELECT id FROM g").await.unwrap().collect().await.unwrap();
    assert_eq!(strings(&batches, 0), vec!["a", "b", "d"], "DOWN row never visible");
    let batches = ctx2
        .sql("SELECT id FROM g WHERE status = 'DOWN'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert!(
        strings(&batches, 0).is_empty(),
        "base filter ANDs with the user predicate"
    );

    // ── 4) native match + geo pushdown (feature `udf`) ──
    #[cfg(feature = "udf")]
    {
        for f in datafusion_opensearch::udf::all_udfs() {
            ctx.register_udf(f.as_ref().clone());
        }
        let batches = ctx
            .sql("SELECT id FROM t WHERE os_match(title, 'engine')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(strings(&batches, 0), vec!["a", "c"], "analyzed match on a text field");
        // 5 km around Helsinki → only 'a'.
        let batches = ctx
            .sql("SELECT id FROM t WHERE os_geo_distance(pos, 24.94, 60.17, 5000)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(strings(&batches, 0), vec!["a"], "geo_distance pushed natively");
        // A bbox over the Nordics, west of Helsinki → Stockholm/Copenhagen/Oslo.
        let batches = ctx
            .sql("SELECT id FROM t WHERE os_geo_bbox(pos, 5.0, 65.0, 20.0, 50.0)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(
            strings(&batches, 0),
            vec!["b", "c", "d"],
            "geo_bounding_box pushed natively"
        );
    }

    http.delete(format!("{url}/{INDEX}")).send().await.unwrap();
}
