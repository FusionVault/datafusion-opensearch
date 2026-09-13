# Changelog

All notable changes to `datafusion-opensearch` are recorded here. The crate tracks the DataFusion
major line (`0.x` ↔ DataFusion 54); a DataFusion major bump is a minor bump here.

## 0.2.0 — 2026-09-13

### Added
- **Streaming scans.** A scan now pages through a scroll cursor (`OpenSearchExec`), emitting one
  `RecordBatch` per page until the result is exhausted or the `LIMIT` is met — complete at any index
  size, memory bounded by `with_page_size` (default 5 000). `with_max_rows` caps LIMIT-less scans.
  `with_partitions(n)` fans a scan out over `n` sliced scrolls, one per DataFusion partition; the
  operator reports `BaselineMetrics` (rows, elapsed time) for `EXPLAIN ANALYZE`.
- **`CREATE EXTERNAL TABLE … STORED AS OPENSEARCH`** via `OpenSearchTableProviderFactory`
  (`LOCATION '<base url>/<index>'`, optional column list, `OPTIONS` `page_size` / `max_rows` /
  `sort` / `base_filter`).
- **`OpenSearchSchemaProvider`**: a cluster's indices as a DataFusion schema (`SHOW TABLES`,
  `SELECT … FROM os."index"`), tables built lazily from their `_mapping`.
- `date` / `date_nanos` mapping types become `Timestamp(Millisecond, UTC)` columns (ISO 8601 or
  epoch-millis values); timestamp and date literals push down as epoch milliseconds.
- `OpenSearchClient::with_basic_auth` / `with_bearer_token`; `OpenSearchClient::indices`; the
  scroll API (`search_page`, `scroll`, `clear_scroll`).
- `OpenSearchTableFactory::provider` (not type-erased) and `client()`;
  `OpenSearchTableProvider::search_body` to inspect what a scan sends; `EXPLAIN VERBOSE` shows it.

### Changed
- `DEFAULT_SIZE` / `with_default_size` (a silent 10 000-row cap on LIMIT-less queries) are replaced
  by `DEFAULT_PAGE_SIZE` / `with_page_size` and `with_max_rows`; a LIMIT-less scan now returns the
  whole result.

## 0.1.0 — 2026-09-08

Initial release.

- `OpenSearchTableProvider`: a DataFusion `TableProvider` over one index with **filter, projection
  and limit pushdown** (`term`/`range`/`terms`/`exists`/`prefix`/`wildcard`, `bool` composition);
  untranslatable filters fall back to DataFusion so partial pushdown is always correct.
- `OpenSearchTableFactory` + `schema_from_mapping`: derive the Arrow schema from the index
  `_mapping` instead of declaring it.
- `with_base_filter`: always-on query-DSL clauses ANDed into every scan (row-level security,
  tenancy, soft-delete).
- `with_sort` / `with_search_after`: sort and cursor pagination pushed into `_search`.
- `OpenSearchClient`: bring-your-own `reqwest::Client` for auth, timeouts, TLS.
- Feature `udf`: `os_match`, `os_geo_bbox`, `os_geo_distance`, `os_geo_polygon` scalar UDFs pushed
  down as native `match` / geo queries.
- testcontainers end-to-end test against a real OpenSearch (`--ignored`, needs Docker).
