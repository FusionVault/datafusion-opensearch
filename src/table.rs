//! The [`TableProvider`], the factory that builds one from an index's `_mapping`, and the
//! [`TableProviderFactory`] behind `CREATE EXTERNAL TABLE … STORED AS OPENSEARCH`.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider, TableProviderFactory};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::{CreateExternalTable, Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use serde_json::{json, Value};

use crate::client::OpenSearchClient;
use crate::exec::OpenSearchExec;
use crate::schema::schema_from_mapping;
use crate::{pushdown, MalformedMappingSnafu, Result};

/// Hits fetched per `_search` page. Bounded above by the index's `max_result_window` (10 000 by
/// default); override per table with [`OpenSearchTableProvider::with_page_size`].
pub const DEFAULT_PAGE_SIZE: usize = 5_000;

/// The `STORED AS` name that selects [`OpenSearchTableProviderFactory`] in `CREATE EXTERNAL TABLE`.
pub const FILE_TYPE: &str = "OPENSEARCH";

/// A DataFusion table backed by one OpenSearch index.
pub struct OpenSearchTableProvider {
    client: OpenSearchClient,
    index: String,
    schema: SchemaRef,
    page_size: usize,
    /// Cap on rows a scan returns when the query has no LIMIT (`None` = the whole result).
    max_rows: Option<usize>,
    /// DataFusion partitions per scan, each a sliced scroll (1 = a plain scroll).
    partitions: usize,
    /// Extra query-DSL clauses ANDed into every scan (see [`with_base_filter`]).
    ///
    /// [`with_base_filter`]: OpenSearchTableProvider::with_base_filter
    base_filter: Vec<Value>,
    /// `sort` pushed into `_search` (field, ascending). Empty → OpenSearch default order.
    sort: Vec<(String, bool)>,
    /// `search_after` values for cursor pagination (must align with `sort`).
    search_after: Option<Vec<Value>>,
}

impl fmt::Debug for OpenSearchTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpenSearchTableProvider")
            .field("index", &self.index)
            .field("fields", &self.schema.fields().len())
            .field("page_size", &self.page_size)
            .field("max_rows", &self.max_rows)
            .field("partitions", &self.partitions)
            .field("base_filter", &self.base_filter.len())
            .finish()
    }
}

impl OpenSearchTableProvider {
    /// `base_url` e.g. `http://localhost:9200`; `index` e.g. `my-index`; `schema` declares the
    /// columns (and Arrow types) to read from each hit's `_source`. Supported column types:
    /// `Utf8`, `Int64`, `Float64`, `Boolean`, `Timestamp(Millisecond, _)` (see
    /// [`crate::schema::build_batch`]).
    pub fn new(base_url: impl Into<String>, index: impl Into<String>, schema: SchemaRef) -> Self {
        Self::with_client_and_schema(OpenSearchClient::new(base_url), index, schema)
    }

    /// Build over an existing [`OpenSearchClient`] (shared connection pool / auth / TLS).
    pub fn with_client_and_schema(client: OpenSearchClient, index: impl Into<String>, schema: SchemaRef) -> Self {
        Self {
            client,
            index: index.into(),
            schema,
            page_size: DEFAULT_PAGE_SIZE,
            max_rows: None,
            partitions: 1,
            base_filter: Vec::new(),
            sort: Vec::new(),
            search_after: None,
        }
    }

    /// Push a sort into `_search` — `(field, ascending)` pairs. Required for stable pagination
    /// (and for a LIMIT to mean "top-N by this order" rather than an arbitrary N).
    pub fn with_sort(mut self, sort: Vec<(String, bool)>) -> Self {
        self.sort = sort;
        self
    }

    /// Continue after a prior page: OpenSearch `search_after` values, aligned with `with_sort`.
    /// A scan then reads exactly one page (of the LIMIT, or the page size) — the loop is yours.
    pub fn with_search_after(mut self, after: Vec<Value>) -> Self {
        self.search_after = Some(after);
        self
    }

    /// Hits per `_search` page while streaming a result (default [`DEFAULT_PAGE_SIZE`]).
    pub fn with_page_size(mut self, n: usize) -> Self {
        self.page_size = n.max(1);
        self
    }

    /// Cap the rows a LIMIT-less scan returns. By default a scan streams the whole result.
    pub fn with_max_rows(mut self, n: Option<usize>) -> Self {
        self.max_rows = n;
        self
    }

    /// Read the index through `n` DataFusion partitions in parallel, each an OpenSearch sliced
    /// scroll (`slice: { id, max }`). Worthwhile for large scans on multi-shard indices; a query
    /// with a LIMIT always uses one partition so the limit is exact. Default 1.
    pub fn with_partitions(mut self, n: usize) -> Self {
        self.partitions = n.max(1);
        self
    }

    /// Add query-DSL clauses that are ANDed into the `bool.filter` of EVERY scan, on top of any
    /// pushed-down WHERE. Use this for constraints the caller must never opt out of — e.g. a
    /// row-level security predicate, a tenancy filter, or a soft-delete flag. The clauses are
    /// opaque OpenSearch DSL (`serde_json::Value`), so this crate stays agnostic to what they express.
    pub fn with_base_filter(mut self, clauses: Vec<Value>) -> Self {
        self.base_filter = clauses;
        self
    }

    /// Supply a preconfigured reqwest client (timeouts, connection pool, TLS roots, headers).
    pub fn with_client(mut self, http: reqwest::Client) -> Self {
        self.client = self.client.with_client(http);
        self
    }

    /// The index this table reads.
    pub fn index(&self) -> &str {
        &self.index
    }

    /// The `_search` body (without `size`) a scan with these filters and this projection sends.
    /// Exposed so callers can see exactly what was pushed down.
    pub fn search_body(&self, projected: &SchemaRef, filters: &[Expr]) -> Value {
        let mut clauses: Vec<Value> = self.base_filter.clone();
        clauses.extend(filters.iter().filter_map(pushdown::expr_to_query));
        let query = if clauses.is_empty() {
            json!({ "match_all": {} })
        } else {
            json!({ "bool": { "filter": clauses } })
        };
        let source_fields: Vec<&str> = projected.fields().iter().map(|f| f.name().as_str()).collect();
        let mut body = json!({ "query": query, "_source": source_fields });
        if !self.sort.is_empty() {
            body["sort"] = Value::Array(
                self.sort
                    .iter()
                    .map(|(f, asc)| json!({ f: { "order": if *asc { "asc" } else { "desc" } } }))
                    .collect(),
            );
        }
        if let Some(after) = &self.search_after {
            body["search_after"] = Value::Array(after.clone());
        }
        body
    }
}

#[async_trait]
impl TableProvider for OpenSearchTableProvider {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// Report `Exact` for filters [`pushdown::expr_to_query`] can translate (DataFusion drops its
    /// Filter node), `Unsupported` for the rest (DataFusion keeps filtering them). Same predicate
    /// as `scan` uses, so the two never disagree.
    fn supports_filters_pushdown(&self, filters: &[&Expr]) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|f| {
                if pushdown::expr_to_query(f).is_some() {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let projected: SchemaRef = match projection {
            Some(p) => Arc::new(self.schema.project(p)?),
            None => self.schema.clone(),
        };
        let body = self.search_body(&projected, filters);
        let fetch = match (limit, self.max_rows) {
            (Some(l), Some(m)) => Some(l.min(m)),
            (l, m) => l.or(m),
        };
        Ok(Arc::new(
            OpenSearchExec::new(self.client.clone(), self.index.clone(), body, projected)
                .with_page_size(self.page_size)
                .with_fetch_limit(fetch)
                .with_single_page(self.search_after.is_some())
                .with_partitions(self.partitions),
        ))
    }
}

/// Builds [`OpenSearchTableProvider`]s over one cluster, deriving each table's schema from the
/// index `_mapping` unless one is declared.
#[derive(Clone, Debug)]
pub struct OpenSearchTableFactory {
    client: OpenSearchClient,
}

impl OpenSearchTableFactory {
    pub fn new(client: OpenSearchClient) -> Self {
        Self { client }
    }

    /// The client every table built here shares.
    pub fn client(&self) -> &OpenSearchClient {
        &self.client
    }

    /// A table over `index` whose schema is derived from its `_mapping`
    /// ([`schema_from_mapping`]). Fails if the index does not exist or has no `properties`.
    pub async fn table_provider(&self, index: impl Into<String>) -> Result<Arc<dyn TableProvider>> {
        Ok(Arc::new(self.provider(index).await?))
    }

    /// Like [`table_provider`](Self::table_provider), not type-erased, for the builder methods.
    pub async fn provider(&self, index: impl Into<String>) -> Result<OpenSearchTableProvider> {
        let index = index.into();
        let mapping = self.client.mapping(&index).await?;
        let schema =
            schema_from_mapping(&mapping).ok_or_else(|| MalformedMappingSnafu { index: index.clone() }.build())?;
        Ok(OpenSearchTableProvider::with_client_and_schema(
            self.client.clone(),
            index,
            schema,
        ))
    }

    /// A table over `index` with a declared schema (no `_mapping` round-trip; only the listed
    /// columns are read, and a not-yet-created index reads as empty).
    pub fn table_provider_with_schema(&self, index: impl Into<String>, schema: SchemaRef) -> Arc<dyn TableProvider> {
        Arc::new(self.provider_with_schema(index, schema))
    }

    /// The provider (not type-erased), for callers that want the builder methods.
    pub fn provider_with_schema(&self, index: impl Into<String>, schema: SchemaRef) -> OpenSearchTableProvider {
        OpenSearchTableProvider::with_client_and_schema(self.client.clone(), index, schema)
    }
}

/// The [`TableProviderFactory`] that makes OpenSearch indices reachable from SQL alone:
///
/// ```sql
/// CREATE EXTERNAL TABLE docs STORED AS OPENSEARCH LOCATION 'http://localhost:9200/my-index';
/// ```
///
/// `LOCATION` is `<base url>/<index>`. Without a column list the schema is derived from the index
/// `_mapping`; with one, only those columns are read. Recognised `OPTIONS`: `page_size`,
/// `max_rows`, `partitions`, `sort` (`"field:asc,other:desc"`), `base_filter` (a JSON array of
/// query-DSL clauses).
/// Credentials and the HTTP client set on the factory apply to every table it creates.
#[derive(Clone, Debug, Default)]
pub struct OpenSearchTableProviderFactory {
    http: Option<reqwest::Client>,
    basic: Option<(String, String)>,
    bearer: Option<String>,
}

impl OpenSearchTableProviderFactory {
    pub fn new() -> Self {
        Self::default()
    }

    /// Use this [`reqwest::Client`] for every table (timeouts, TLS roots, headers).
    pub fn with_client(mut self, http: reqwest::Client) -> Self {
        self.http = Some(http);
        self
    }

    /// Send HTTP basic credentials with every request.
    pub fn with_basic_auth(mut self, user: impl Into<String>, password: impl Into<String>) -> Self {
        self.basic = Some((user.into(), password.into()));
        self
    }

    /// Send a bearer token with every request.
    pub fn with_bearer_token(mut self, token: impl Into<String>) -> Self {
        self.bearer = Some(token.into());
        self
    }

    /// Register this factory on a [`SessionContext`] as `STORED AS OPENSEARCH`.
    pub fn register(self, ctx: &SessionContext) {
        ctx.state_ref()
            .write()
            .table_factories_mut()
            .insert(FILE_TYPE.to_string(), Arc::new(self));
    }

    fn client_for(&self, base_url: &str) -> OpenSearchClient {
        let mut c = OpenSearchClient::new(base_url);
        if let Some(http) = &self.http {
            c = c.with_client(http.clone());
        }
        if let Some((u, p)) = &self.basic {
            c = c.with_basic_auth(u, p);
        }
        if let Some(t) = &self.bearer {
            c = c.with_bearer_token(t);
        }
        c
    }
}

/// Split `LOCATION` into `(base_url, index)`: the index is the last path segment.
fn split_location(location: &str) -> DataFusionResult<(&str, &str)> {
    let trimmed = location.trim_end_matches('/');
    let (base, index) = trimmed.rsplit_once('/').ok_or_else(|| {
        DataFusionError::Plan(format!(
            "datafusion-opensearch: LOCATION must be '<base url>/<index>', got '{location}'"
        ))
    })?;
    if !base.contains("://") || index.is_empty() || base.ends_with(':') || base.ends_with('/') {
        return Err(DataFusionError::Plan(format!(
            "datafusion-opensearch: LOCATION must be '<base url>/<index>', got '{location}'"
        )));
    }
    Ok((base, index))
}

/// Parse `"field:asc,other:desc"` (direction optional, default ascending).
fn parse_sort(spec: &str) -> DataFusionResult<Vec<(String, bool)>> {
    spec.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| match s.rsplit_once(':') {
            Some((f, "asc")) => Ok((f.trim().to_string(), true)),
            Some((f, "desc")) => Ok((f.trim().to_string(), false)),
            Some((_, other)) => Err(DataFusionError::Plan(format!(
                "datafusion-opensearch: sort direction must be asc or desc, got '{other}'"
            ))),
            None => Ok((s.to_string(), true)),
        })
        .collect()
}

fn parse_usize(options: &std::collections::HashMap<String, String>, key: &str) -> DataFusionResult<Option<usize>> {
    options
        .get(key)
        .map(|v| {
            v.trim().parse::<usize>().map_err(|_| {
                DataFusionError::Plan(format!(
                    "datafusion-opensearch: option '{key}' must be an integer, got '{v}'"
                ))
            })
        })
        .transpose()
}

#[async_trait]
impl TableProviderFactory for OpenSearchTableProviderFactory {
    async fn create(
        &self,
        _state: &dyn Session,
        cmd: &CreateExternalTable,
    ) -> DataFusionResult<Arc<dyn TableProvider>> {
        let (base_url, index) = split_location(&cmd.location)?;
        // DataFusion qualifies unprefixed OPTIONS keys with `format.`; accept both spellings.
        let options: std::collections::HashMap<String, String> = cmd
            .options
            .iter()
            .map(|(k, v)| (k.strip_prefix("format.").unwrap_or(k).to_string(), v.clone()))
            .collect();
        let factory = OpenSearchTableFactory::new(self.client_for(base_url));
        let mut provider = if cmd.schema.fields().is_empty() {
            factory.provider(index).await?
        } else {
            factory.provider_with_schema(index, Arc::new(cmd.schema.as_arrow().clone()))
        };
        if let Some(n) = parse_usize(&options, "page_size")? {
            provider = provider.with_page_size(n);
        }
        if let Some(n) = parse_usize(&options, "max_rows")? {
            provider = provider.with_max_rows(Some(n));
        }
        if let Some(n) = parse_usize(&options, "partitions")? {
            provider = provider.with_partitions(n);
        }
        if let Some(sort) = options.get("sort") {
            provider = provider.with_sort(parse_sort(sort)?);
        }
        if let Some(raw) = options.get("base_filter") {
            let clauses: Vec<Value> = serde_json::from_str(raw).map_err(|e| {
                DataFusionError::Plan(format!(
                    "datafusion-opensearch: option 'base_filter' must be a JSON array of query clauses: {e}"
                ))
            })?;
            provider = provider.with_base_filter(clauses);
        }
        Ok(Arc::new(provider))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::logical_expr::{col, lit};

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("status", DataType::Utf8, true),
            Field::new("speed", DataType::Float64, true),
            Field::new("active", DataType::Boolean, true),
        ]))
    }

    #[test]
    fn search_body_composes_base_filter_pushdown_projection_and_sort() {
        let p = OpenSearchTableProvider::new("http://x", "i", schema())
            .with_base_filter(vec![json!({ "term": { "tenant": "t1" } })])
            .with_sort(vec![("speed".into(), false)]);
        let projected = Arc::new(schema().project(&[0]).unwrap());
        let body = p.search_body(&projected, &[col("status").eq(lit("OK")), col("a").eq(col("b"))]);
        assert_eq!(
            body["query"],
            json!({ "bool": { "filter": [ { "term": { "tenant": "t1" } }, { "term": { "status": "OK" } } ] } })
        );
        assert_eq!(body["_source"], json!(["id"]));
        assert_eq!(body["sort"], json!([{ "speed": { "order": "desc" } }]));
        assert!(body.get("size").is_none(), "size belongs to the streaming exec");
        let plain = OpenSearchTableProvider::new("http://x", "i", schema()).search_body(&schema(), &[]);
        assert_eq!(plain["query"], json!({ "match_all": {} }));
        assert_eq!(p.index(), "i");
    }

    #[test]
    fn factory_builds_a_declared_schema_provider_without_io() {
        let f = OpenSearchTableFactory::new(OpenSearchClient::new("http://x:9200"));
        let p = f
            .provider_with_schema("idx", schema())
            .with_page_size(5)
            .with_max_rows(Some(50));
        assert_eq!(p.page_size, 5);
        assert_eq!(p.max_rows, Some(50));
        assert_eq!(f.table_provider_with_schema("idx", schema()).schema().fields().len(), 4);
        assert_eq!(f.client().base_url(), "http://x:9200");
    }

    #[test]
    fn location_splits_into_base_url_and_index() {
        assert_eq!(
            split_location("http://localhost:9200/my-index").unwrap(),
            ("http://localhost:9200", "my-index")
        );
        assert_eq!(
            split_location("https://os.example.com/logs-2024/").unwrap(),
            ("https://os.example.com", "logs-2024")
        );
        assert!(split_location("my-index").is_err());
        assert!(split_location("http://localhost:9200/").is_err());
        assert!(split_location("http://localhost:9200").is_err());
    }

    #[test]
    fn sort_option_parses() {
        assert_eq!(
            parse_sort("ts:desc, id").unwrap(),
            vec![("ts".to_string(), false), ("id".to_string(), true)]
        );
        assert!(parse_sort("ts:sideways").is_err());
    }

    #[tokio::test]
    async fn create_external_table_with_declared_columns_needs_no_io() {
        let ctx = SessionContext::new();
        OpenSearchTableProviderFactory::new()
            .with_basic_auth("u", "p")
            .register(&ctx);
        ctx.sql(
            "CREATE EXTERNAL TABLE docs (id VARCHAR, speed DOUBLE) STORED AS OPENSEARCH \
             LOCATION 'http://localhost:9200/my-index' OPTIONS ('page_size' '100', 'sort' 'speed:desc')",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
        let t = ctx.table_provider("docs").await.unwrap();
        assert_eq!(t.schema().fields().len(), 2);
        let any: &dyn std::any::Any = t.as_ref();
        let p = any.downcast_ref::<OpenSearchTableProvider>().unwrap();
        assert_eq!(p.page_size, 100);
        assert_eq!(p.sort, vec![("speed".to_string(), false)]);
        assert_eq!(p.index(), "my-index");
    }
}
