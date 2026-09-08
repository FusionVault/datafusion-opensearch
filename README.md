# datafusion-opensearch

A [DataFusion](https://datafusion.apache.org/) `TableProvider` over
[OpenSearch](https://opensearch.org/) (and Elasticsearch), with **filter, projection and limit
pushdown** and **schema inference from the index mapping**.

Register an index as a DataFusion table and query it with SQL or the DataFrame API. The planner's
`WHERE` is pushed into the OpenSearch query DSL, the projection into `_source`, and `LIMIT` into
`size`. Anything DataFusion asks for that can't be translated exactly is applied by DataFusion
in-memory, so results are always correct — pushdown only ever *narrows* the OpenSearch request.

```rust
use datafusion::prelude::SessionContext;
use datafusion_opensearch::{OpenSearchClient, OpenSearchTableFactory};

let factory = OpenSearchTableFactory::new(OpenSearchClient::new("http://localhost:9200"));
let table = factory.table_provider("my-index").await?; // schema derived from _mapping

let ctx = SessionContext::new();
ctx.register_table("docs", table)?;
let df = ctx.sql("SELECT id, status FROM docs WHERE speed > 40 AND status = 'OK' LIMIT 100").await?;
df.show().await?;
```

Prefer to declare the columns yourself (only those are read, and a not-yet-created index reads as
empty)? Use `OpenSearchTableProvider::new(url, index, schema)`.

## Pushdown coverage

| SQL | OpenSearch |
|---|---|
| `col = v`, `col != v` | `term`, `bool.must_not.term` |
| `col > / >= / < / <= v`, `col BETWEEN a AND b` | `range` |
| `col IN (…)`, `col NOT IN (…)` | `terms`, `bool.must_not.terms` |
| `col IS [NOT] NULL` | `exists` / `bool.must_not.exists` |
| `col LIKE 'v%'` / `col LIKE '%v_'` / `ILIKE` | `prefix` / `wildcard` (+ `case_insensitive`) |
| `AND` / `OR` / `NOT` | `bool.filter` / `bool.should` / `bool.must_not` |

Literal-first operands (`40 < col`) are normalized. Anything else (functions or casts on columns,
`col = col`, `= NULL`) is left to DataFusion.

## Native predicates (feature `udf`)

Some of the most useful OpenSearch queries have no SQL spelling. With the `udf` feature they are
scalar UDFs the provider pushes down as native queries:

| UDF | OpenSearch query |
|---|---|
| `os_match(field, 'query')` | analyzed `match` |
| `os_geo_bbox(field, tl_lon, tl_lat, br_lon, br_lat)` | `geo_bounding_box` |
| `os_geo_distance(field, lon, lat, radius_metres)` | `geo_distance` |
| `os_geo_polygon(field, '[[lon,lat],…]')` | `geo_polygon` |

```rust
for f in datafusion_opensearch::udf::all_udfs() { ctx.register_udf(f.as_ref().clone()); }
ctx.sql("SELECT id FROM docs WHERE os_match(title, 'engine fault') AND os_geo_distance(pos, 24.94, 60.17, 5000)").await?;
```

Pushdown recognises the UDFs **by name**, so in a distributed setup every planning and executing
node must register them (plans serialise scalar functions by name).

## Schema

`schema_from_mapping` maps OpenSearch field types to Arrow: text-like and temporal types → `Utf8`,
integer types → `Int64`, floating types → `Float64`, `boolean` → `Boolean`; `object`, `nested`,
`geo_point`, `geo_shape` and unknown types → `Utf8` holding the value's JSON text. Only those four
Arrow types are materialised; absent, null or type-mismatched values become nulls.

## Always-on constraints

`with_base_filter(clauses)` ANDs opaque query-DSL clauses into every scan — for row-level
security, tenancy, or soft-delete predicates the caller must not be able to opt out of.
`with_sort` / `with_search_after` push sorting and cursor pagination into `_search`.
`OpenSearchClient::with_client` accepts your own `reqwest::Client` for auth, timeouts and TLS.

## Compatibility

Built against DataFusion `54` (the crate tracks the DataFusion major line). Tested against
OpenSearch 2.x; the query DSL used is shared with Elasticsearch 7/8.

## Testing

Unit tests need nothing. The end-to-end test starts a real OpenSearch with testcontainers (Docker):

```
cargo test -p datafusion-opensearch --all-features --test testcontainers -- --ignored
```

## License

Apache-2.0.
