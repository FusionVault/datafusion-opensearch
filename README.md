# datafusion-opensearch

[![crates.io](https://img.shields.io/crates/v/datafusion-opensearch.svg)](https://crates.io/crates/datafusion-opensearch)
[![docs.rs](https://docs.rs/datafusion-opensearch/badge.svg)](https://docs.rs/datafusion-opensearch)
[![ci](https://github.com/FusionVault/datafusion-opensearch/actions/workflows/ci.yml/badge.svg)](https://github.com/FusionVault/datafusion-opensearch/actions/workflows/ci.yml)

Query [OpenSearch](https://opensearch.org/) (or Elasticsearch) indices with SQL and the DataFrame
API through [Apache DataFusion](https://datafusion.apache.org/).

An index becomes a DataFusion table. Your `WHERE` is translated into the OpenSearch query DSL, the
projection into `_source`, the `LIMIT` into the page size, and the result streams back page by page
through a scroll cursor — so a scan is complete at any index size and memory stays bounded. Anything
DataFusion asks for that cannot be translated exactly is applied by DataFusion in memory, so results
are always correct: pushdown only ever narrows the request, never widens it.

## Install

```bash
cargo add datafusion-opensearch
```

Add `--features udf` for OpenSearch-native full-text and geo predicates as SQL functions.

## Quick start

SQL only — register the factory once, then point `CREATE EXTERNAL TABLE` at `<cluster>/<index>`:

```rust,no_run
use datafusion::prelude::SessionContext;
use datafusion_opensearch::OpenSearchTableProviderFactory;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();
    OpenSearchTableProviderFactory::new().register(&ctx);

    ctx.sql("CREATE EXTERNAL TABLE docs STORED AS OPENSEARCH LOCATION 'http://localhost:9200/my-index'")
        .await?
        .collect()
        .await?;

    // WHERE → query DSL, the two columns → _source, LIMIT → page size.
    ctx.sql("SELECT id, status FROM docs WHERE speed > 40 AND status = 'OK' LIMIT 100")
        .await?
        .show()
        .await?;
    Ok(())
}
```

The column list is optional: without one the Arrow schema is derived from the index `_mapping`.

Every Rust example in this README is compiled by CI, so they are safe to copy.

## Guide

### Three ways to register an index

**From SQL**, as above. `LOCATION` is `<base url>/<index>`; `OPTIONS` accepts `page_size`,
`max_rows`, `sort` (`'ts:desc,id:asc'`) and `base_filter` (a JSON array of query-DSL clauses).
Credentials set on the factory apply to every table it creates:

```rust,no_run
use datafusion::prelude::SessionContext;
use datafusion_opensearch::OpenSearchTableProviderFactory;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();
    OpenSearchTableProviderFactory::new()
        .with_basic_auth("admin", "admin")
        .register(&ctx);
    ctx.sql(
        "CREATE EXTERNAL TABLE events (id VARCHAR, ts TIMESTAMP, level VARCHAR) \
         STORED AS OPENSEARCH LOCATION 'https://os.example.com/events' \
         OPTIONS ('sort' 'ts:desc', 'max_rows' '100000')",
    )
    .await?
    .collect()
    .await?;
    Ok(())
}
```

**A whole cluster** — every non-system index becomes a table, listed by `SHOW TABLES` (enable
DataFusion's `information_schema` for that) and built lazily from its `_mapping` the first time it
is queried:

```rust,no_run
use std::sync::Arc;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_opensearch::{OpenSearchClient, OpenSearchSchemaProvider};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_information_schema(true));
    let os = OpenSearchSchemaProvider::connect(OpenSearchClient::new("http://localhost:9200")).await?;
    ctx.catalog("datafusion").unwrap().register_schema("os", Arc::new(os))?;

    ctx.sql("SHOW TABLES").await?.show().await?;
    ctx.sql("SELECT count(*) FROM os.\"my-index\"").await?.show().await?;
    Ok(())
}
```

**One index, from Rust** — with the schema inferred, or declared so only the listed columns are
read (and an index that does not exist yet reads as empty):

```rust,no_run
use std::sync::Arc;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::SessionContext;
use datafusion_opensearch::{OpenSearchClient, OpenSearchTableFactory, OpenSearchTableProvider};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();

    // Inferred from _mapping.
    let factory = OpenSearchTableFactory::new(OpenSearchClient::new("http://localhost:9200"));
    ctx.register_table("docs", factory.table_provider("my-index").await?)?;

    // Declared.
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, true),
        Field::new("speed", DataType::Float64, true),
    ]));
    ctx.register_table("fast", Arc::new(OpenSearchTableProvider::new("http://localhost:9200", "my-index", schema)))?;

    let df = ctx.table("fast").await?.filter(datafusion::prelude::col("speed").gt(datafusion::prelude::lit(40.0)))?;
    df.show().await?;
    Ok(())
}
```

### What is pushed down

The provider translates these into the query DSL and tells DataFusion the filter is fully handled:

- `col = v`, `col != v` → `term` / `must_not term`
- `col > v`, `>=`, `<`, `<=`, `BETWEEN` → `range` (a literal on the left, `40 < col`, is normalised)
- `col IN (…)`, `NOT IN` → `terms`
- `col IS NULL`, `IS NOT NULL` → `exists`
- `col LIKE 'abc%'` → `prefix`; any other `LIKE` / `ILIKE` → `wildcard` (case-insensitive when `ILIKE`)
- `AND`, `OR`, `NOT` → `bool.filter` / `bool.should` / `bool.must_not`, all-or-nothing per branch
- comparisons on timestamp columns → epoch milliseconds, which OpenSearch's default `date` format accepts
- the projection → `_source`; `LIMIT` → the page size and a stop after that many rows

Everything else — a function or cast on a column, `col = col`, `= NULL` — stays in DataFusion and is
evaluated on the rows that come back. An `OR` with one untranslatable side is not pushed at all,
because pushing half of it would widen the result.

`EXPLAIN` shows the operator (`OpenSearchExec`) and, in verbose mode, the exact `_search` body that was sent.

### Native predicates (feature `udf`)

Some of the most useful OpenSearch queries have no SQL spelling. With the `udf` feature they are
scalar functions the provider pushes down as native queries:

- `os_match(field, 'query')` — the analysed full-text `match`
- `os_geo_distance(field, lon, lat, radius_metres)` — `geo_distance`
- `os_geo_bbox(field, top_left_lon, top_left_lat, bottom_right_lon, bottom_right_lat)` — `geo_bounding_box`
- `os_geo_polygon(field, '[[lon,lat],[lon,lat],…]')` — `geo_polygon`

```rust,no_run
use datafusion::prelude::SessionContext;
use datafusion_opensearch::OpenSearchTableProviderFactory;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();
    OpenSearchTableProviderFactory::new().register(&ctx);
    #[cfg(feature = "udf")]
    for f in datafusion_opensearch::udf::all_udfs() {
        ctx.register_udf(f.as_ref().clone());
    }
    ctx.sql("CREATE EXTERNAL TABLE docs STORED AS OPENSEARCH LOCATION 'http://localhost:9200/my-index'")
        .await?
        .collect()
        .await?;
    ctx.sql("SELECT id FROM docs WHERE os_match(title, 'engine fault') AND os_geo_distance(pos, 24.94, 60.17, 5000)")
        .await?
        .show()
        .await?;
    Ok(())
}
```

Pushdown recognises the functions by name, so in a distributed setup every node that plans or
executes must register them.

### Streaming, limits and pagination

- A scan streams the whole result through a scroll cursor, one `RecordBatch` per page. Set the page
  size with `with_page_size` (default 5 000; the index's `max_result_window`, 10 000 by default, is
  the ceiling).
- A `LIMIT` stops the scan after that many rows. `with_max_rows` caps LIMIT-less scans if you need
  a safety net.
- `with_sort` pushes an order into `_search`, which makes `LIMIT n` mean "the first n by that order".
- For caller-driven cursor pagination, `with_search_after` reads exactly one page; feed the last
  row's sort values back in for the next one.
- `with_partitions(n)` (or `OPTIONS ('partitions' '4')`) reads through `n` DataFusion partitions in
  parallel, each an OpenSearch sliced scroll — worthwhile for large scans over multi-shard indices.
  A query with a `LIMIT` always uses one partition so the limit is exact.
- `EXPLAIN ANALYZE` reports the operator's output rows and elapsed time per partition.

```rust,no_run
use std::sync::Arc;
use datafusion::prelude::SessionContext;
use datafusion_opensearch::{OpenSearchClient, OpenSearchTableFactory};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let factory = OpenSearchTableFactory::new(OpenSearchClient::new("http://localhost:9200"));
    let table = factory
        .provider("events")
        .await?
        .with_sort(vec![("ts".into(), false)])
        .with_page_size(1_000)
        .with_max_rows(Some(50_000));
    let ctx = SessionContext::new();
    ctx.register_table("events", Arc::new(table))?;
    ctx.sql("SELECT id, ts FROM events WHERE level = 'ERROR' LIMIT 20").await?.show().await?;
    Ok(())
}
```

### Always-on constraints

`with_base_filter` ANDs query-DSL clauses into every scan, beneath whatever the query asks for.
Use it for row-level security, tenancy or soft-delete predicates the caller must never be able to
opt out of. The clauses are opaque OpenSearch DSL, so anything the engine can express works:

```rust,no_run
use std::sync::Arc;
use datafusion::prelude::SessionContext;
use datafusion_opensearch::{OpenSearchClient, OpenSearchTableFactory};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let factory = OpenSearchTableFactory::new(OpenSearchClient::new("http://localhost:9200"));
    let tenant_only = factory
        .provider("orders")
        .await?
        .with_base_filter(vec![json!({ "term": { "tenant": "acme" } }), json!({ "term": { "deleted": false } })]);
    let ctx = SessionContext::new();
    ctx.register_table("orders", Arc::new(tenant_only))?;
    // Every query on `orders` is scoped to acme's live rows, whatever its WHERE says.
    ctx.sql("SELECT count(*) FROM orders").await?.show().await?;
    Ok(())
}
```

### Authentication and the HTTP client

```rust,no_run
use datafusion_opensearch::OpenSearchClient;

let with_password = OpenSearchClient::new("https://os.example.com").with_basic_auth("admin", "admin");
let with_token = OpenSearchClient::new("https://os.example.com").with_bearer_token("eyJ…");

// Or bring your own reqwest client for timeouts, TLS roots and custom headers.
let http = reqwest::Client::builder().timeout(std::time::Duration::from_secs(30)).build().unwrap();
let tuned = OpenSearchClient::new("https://os.example.com").with_client(http);
```

The client is rustls-only: no OpenSSL to link.

### How mapping types become Arrow types

- `keyword`, `text`, `wildcard`, `constant_keyword`, `ip` → `Utf8`
- `long`, `integer`, `short`, `byte`, `unsigned_long` → `Int64` (a whole-valued float such as `106.0` is accepted)
- `double`, `float`, `half_float`, `scaled_float` → `Float64`
- `boolean` → `Boolean`
- `date`, `date_nanos` → `Timestamp(Millisecond, UTC)`, read from ISO 8601 strings or epoch milliseconds
- `object`, `nested`, `geo_point`, `geo_shape` and anything else → `Utf8` holding the value's JSON text, to parse downstream

Every column is nullable; a field that is absent, null or of the wrong shape becomes null. When you
declare the schema yourself (in Rust or in `CREATE EXTERNAL TABLE`), any string flavour (`Utf8`,
`Utf8View`, `LargeUtf8` — SQL `VARCHAR`), any integer or float width (`INT`, `BIGINT`, `SMALLINT`,
`FLOAT`, `DOUBLE`, …), `BOOLEAN` and `TIMESTAMP` are read; integers out of the declared range
become null rather than wrapping.

### Errors

Anything the cluster returns other than success surfaces as a `DataFusionError::External` wrapping
this crate's `Error`, which carries the URL, the HTTP status and the response body so the cause is
in the message. Two deliberate exceptions: searching an index that does not exist reads as empty
(a table can be registered before its index is created), while asking for the `_mapping` of a
missing index is an error (there is no schema to fall back to).

## Compatibility

Built against DataFusion `54`; the crate tracks the DataFusion major line, so a DataFusion major
bump is a minor bump here. Tested end to end against OpenSearch 2.x; the query DSL, scroll API and
`_cat/indices` used here are shared with Elasticsearch 7 and 8.

## Develop

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features                                         # unit + doc tests, no server needed
cargo test --all-features --test testcontainers -- --ignored      # end to end against a real OpenSearch (Docker)
OS_URL=http://localhost:9200 OS_INDEX=my-index cargo run --example basic
```

CI runs all of the above, including the end-to-end test, on every change, plus a build on the
declared minimum Rust version.

## Versioning and license

Plain semver on the DataFusion major line; tags `vX.Y.Z` at the published commit; see
[CHANGELOG.md](CHANGELOG.md). Apache-2.0, see [LICENSE](LICENSE).
