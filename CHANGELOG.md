# Changelog

All notable changes to `datafusion-opensearch` are recorded here. The crate tracks the DataFusion
major line (`0.x` ↔ DataFusion 54); a DataFusion major bump is a minor bump here.

## Unreleased

### Added
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
