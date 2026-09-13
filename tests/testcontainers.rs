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

use datafusion::arrow::array::{Array, StringArray, StringViewArray};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::catalog::SchemaProvider;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_opensearch::{OpenSearchClient, OpenSearchTableFactory};
use datafusion_opensearch::{OpenSearchSchemaProvider, OpenSearchTableProviderFactory};
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

/// Collect one string column (Utf8 or Utf8View — SQL `VARCHAR` plans as the latter) across all
/// batches, sorted.
fn strings(batches: &[datafusion::arrow::record_batch::RecordBatch], col: usize) -> Vec<String> {
    let mut out = vec![];
    for b in batches {
        let c = b.column(col);
        if let Some(a) = c.as_any().downcast_ref::<StringArray>() {
            out.extend((0..a.len()).filter(|&i| !a.is_null(i)).map(|i| a.value(i).to_string()));
        } else if let Some(a) = c.as_any().downcast_ref::<StringViewArray>() {
            out.extend((0..a.len()).filter(|&i| !a.is_null(i)).map(|i| a.value(i).to_string()));
        } else {
            panic!("column {col} is not a string column: {:?}", c.data_type());
        }
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
            "when": { "type": "date" },
        } } }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    let docs = [
        json!({ "id": "a", "status": "OK",   "speed": 55.0, "count": 1, "active": true,  "title": "engine fault cleared",  "pos": { "lat": 60.17, "lon": 24.94 }, "when": "2024-01-01T10:00:00Z" }), // Helsinki
        json!({ "id": "b", "status": "OK",   "speed": 30.0, "count": 2, "active": false, "title": "routine service",        "pos": { "lat": 59.33, "lon": 18.07 }, "when": "2024-01-02T10:00:00Z" }), // Stockholm
        json!({ "id": "c", "status": "DOWN", "speed": 70.0, "count": 3, "active": true,  "title": "engine overheating",     "pos": { "lat": 55.68, "lon": 12.57 }, "when": "2024-01-03T10:00:00Z" }), // Copenhagen
        json!({ "id": "d", "status": "OK",   "speed": 45.0, "count": 4, "active": true,  "title": "brakes worn",            "pos": { "lat": 59.91, "lon": 10.75 }, "when": 1_704_362_400_000_i64 }), // Oslo, epoch millis = 2024-01-04T10:00:00Z
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
            (
                "when".into(),
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
            ),
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

    // ── 4) timestamps: read from ISO strings and epoch millis; range pushed as epoch millis ──
    let batches = ctx
        .sql("SELECT id FROM t WHERE \"when\" >= '2024-01-03T00:00:00Z'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        strings(&batches, 0),
        vec!["c", "d"],
        "date range pushed as epoch millis"
    );
    let batches = ctx
        .sql("SELECT id FROM t WHERE \"when\" BETWEEN '2024-01-02' AND '2024-01-02T23:59:59Z'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(strings(&batches, 0), vec!["b"]);

    // ── 5) streaming: a result larger than one page comes back complete, LIMIT stops early ──
    let big = format!("{INDEX}-paging");
    http.put(format!("{url}/{big}"))
        .json(&json!({ "mappings": { "properties": { "n": { "type": "integer" } } } }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    let mut bulk = String::new();
    for n in 0..23 {
        bulk.push_str(&format!("{{\"index\":{{\"_id\":\"{n}\"}}}}\n{{\"n\":{n}}}\n"));
    }
    http.post(format!("{url}/{big}/_bulk?refresh=true"))
        .header("content-type", "application/x-ndjson")
        .body(bulk)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    let paged = factory.provider(&big).await.unwrap().with_page_size(5);
    let ctx3 = SessionContext::new();
    ctx3.register_table("p", Arc::new(paged)).unwrap();
    let batches = ctx3.sql("SELECT n FROM p").await.unwrap().collect().await.unwrap();
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 23, "every page of a 23-row result with page_size 5");
    assert!(batches.len() >= 5, "streamed as pages, got {} batches", batches.len());
    let n: usize = ctx3
        .sql("SELECT n FROM p LIMIT 7")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .iter()
        .map(|b| b.num_rows())
        .sum();
    assert_eq!(n, 7, "LIMIT stops the scroll after two pages");
    let capped = factory
        .provider(&big)
        .await
        .unwrap()
        .with_page_size(5)
        .with_max_rows(Some(12));
    ctx3.register_table("capped", Arc::new(capped)).unwrap();
    let n: usize = ctx3
        .sql("SELECT n FROM capped")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .iter()
        .map(|b| b.num_rows())
        .sum();
    assert_eq!(n, 12, "max_rows caps a LIMIT-less scan");
    // Partitioned: three sliced scrolls in parallel still return every row exactly once.
    let sliced = factory
        .provider(&big)
        .await
        .unwrap()
        .with_page_size(4)
        .with_partitions(3);
    ctx3.register_table("sliced", Arc::new(sliced)).unwrap();
    let batches = ctx3.sql("SELECT n FROM sliced").await.unwrap().collect().await.unwrap();
    let mut seen: Vec<i64> = batches
        .iter()
        .flat_map(|b| {
            let a = b
                .column(0)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Int64Array>()
                .unwrap();
            (0..a.len()).map(|i| a.value(i)).collect::<Vec<_>>()
        })
        .collect();
    seen.sort();
    assert_eq!(
        seen,
        (0..23).collect::<Vec<i64>>(),
        "sliced scroll covers every row once"
    );
    let n: usize = ctx3
        .sql("SELECT n FROM sliced LIMIT 5")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .iter()
        .map(|b| b.num_rows())
        .sum();
    assert_eq!(n, 5, "a LIMIT on a partitioned table is exact (single partition)");
    let plan = ctx3
        .sql("EXPLAIN SELECT n FROM sliced")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert!(
        strings(&plan, 1).iter().any(|l| l.contains("partitions=3")),
        "EXPLAIN shows the operator"
    );

    let total: i64 = {
        let b = ctx3.sql("SELECT sum(n) FROM p").await.unwrap().collect().await.unwrap();
        b[0].column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .unwrap()
            .value(0)
    };
    assert_eq!(total, (0..23).sum::<i64>(), "aggregation over a streamed scan");

    // ── 6) CREATE EXTERNAL TABLE … STORED AS OPENSEARCH ──
    // information_schema on, so SHOW TABLES works in step 7.
    let ctx4 = SessionContext::new_with_config(SessionConfig::new().with_information_schema(true));
    OpenSearchTableProviderFactory::new().register(&ctx4);
    ctx4.sql(&format!(
        "CREATE EXTERNAL TABLE docs STORED AS OPENSEARCH LOCATION '{url}/{INDEX}' \
         OPTIONS ('sort' 'speed:desc', 'base_filter' '[{{\"term\":{{\"status\":\"OK\"}}}}]')"
    ))
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();
    let batches = ctx4
        .sql("SELECT id FROM docs LIMIT 2")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        strings(&batches, 0),
        vec!["a", "d"],
        "top-2 by speed among OK rows: a (55), d (45)"
    );
    ctx4.sql(&format!(
        "CREATE EXTERNAL TABLE typed (id VARCHAR, speed DOUBLE) STORED AS OPENSEARCH LOCATION '{url}/{INDEX}'"
    ))
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();
    let batches = ctx4
        .sql("SELECT id FROM typed WHERE speed < 40")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(strings(&batches, 0), vec!["b"]);

    // ── 7) the cluster as a schema: SHOW TABLES lists the indices, tables build lazily ──
    let os = OpenSearchSchemaProvider::connect(OpenSearchClient::new(&url))
        .await
        .unwrap();
    assert!(os.table_exist(INDEX) && os.table_exist(&big));
    ctx4.catalog("datafusion")
        .unwrap()
        .register_schema("os", Arc::new(os))
        .unwrap();
    let batches = ctx4
        .sql(&format!("SELECT id FROM os.\"{INDEX}\" WHERE status = 'DOWN'"))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(strings(&batches, 0), vec!["c"]);
    let names = strings(&ctx4.sql("SHOW TABLES").await.unwrap().collect().await.unwrap(), 2);
    assert!(names.contains(&INDEX.to_string()) && names.contains(&big), "{names:?}");
    http.delete(format!("{url}/{big}")).send().await.unwrap();

    // ── 8) native match + geo pushdown (feature `udf`) ──
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
