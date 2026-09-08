//! The [`TableProvider`] and the factory that builds one from an index's `_mapping`.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::error::Result as DataFusionResult;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use serde_json::{json, Value};

use crate::client::OpenSearchClient;
use crate::schema::{build_batch, schema_from_mapping};
use crate::{pushdown, MalformedMappingSnafu, Result};

/// Rows fetched when a query has no LIMIT — a scan of an unbounded index must not try to pull
/// everything into memory. Override per provider with [`OpenSearchTableProvider::with_default_size`].
pub const DEFAULT_SIZE: usize = 10_000;

/// A DataFusion table backed by one OpenSearch index.
pub struct OpenSearchTableProvider {
    client: OpenSearchClient,
    index: String,
    schema: SchemaRef,
    default_size: usize,
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
            .field("base_filter", &self.base_filter.len())
            .finish()
    }
}

impl OpenSearchTableProvider {
    /// `base_url` e.g. `http://localhost:9200`; `index` e.g. `my-index`; `schema` declares the
    /// columns (and Arrow types) to read from each hit's `_source`. Supported column types:
    /// `Utf8`, `Int64`, `Float64`, `Boolean` (see [`crate::schema::build_batch`]).
    pub fn new(base_url: impl Into<String>, index: impl Into<String>, schema: SchemaRef) -> Self {
        Self::with_client_and_schema(OpenSearchClient::new(base_url), index, schema)
    }

    /// Build over an existing [`OpenSearchClient`] (shared connection pool / auth / TLS).
    pub fn with_client_and_schema(client: OpenSearchClient, index: impl Into<String>, schema: SchemaRef) -> Self {
        Self {
            client,
            index: index.into(),
            schema,
            default_size: DEFAULT_SIZE,
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
    pub fn with_search_after(mut self, after: Vec<Value>) -> Self {
        self.search_after = Some(after);
        self
    }

    /// Set the default row cap for LIMIT-less queries.
    pub fn with_default_size(mut self, n: usize) -> Self {
        self.default_size = n;
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

    /// Supply a preconfigured reqwest client (auth headers, timeouts, connection pool, TLS roots).
    pub fn with_client(mut self, http: reqwest::Client) -> Self {
        self.client = self.client.with_client(http);
        self
    }

    /// The index this table reads.
    pub fn index(&self) -> &str {
        &self.index
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

        // base_filter (always applied) + the translatable pushed-down WHERE clauses.
        let mut clauses: Vec<Value> = self.base_filter.clone();
        clauses.extend(filters.iter().filter_map(pushdown::expr_to_query));
        let query = if clauses.is_empty() {
            json!({ "match_all": {} })
        } else {
            json!({ "bool": { "filter": clauses } })
        };
        let source_fields: Vec<&str> = projected.fields().iter().map(|f| f.name().as_str()).collect();
        let mut body = json!({
            "size": limit.unwrap_or(self.default_size),
            "query": query,
            "_source": source_fields,
        });
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

        let sources = self.client.search(&self.index, &body).await?;
        let batch = build_batch(&projected, &sources)?;
        let exec = MemorySourceConfig::try_new_exec(&[vec![batch]], projected, None)?;
        Ok(exec as Arc<dyn ExecutionPlan>)
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

    /// A table over `index` whose schema is derived from its `_mapping`
    /// ([`schema_from_mapping`]). Fails if the index does not exist or has no `properties`.
    pub async fn table_provider(&self, index: impl Into<String>) -> Result<Arc<dyn TableProvider>> {
        let index = index.into();
        let mapping = self.client.mapping(&index).await?;
        let schema =
            schema_from_mapping(&mapping).ok_or_else(|| MalformedMappingSnafu { index: index.clone() }.build())?;
        Ok(Arc::new(OpenSearchTableProvider::with_client_and_schema(
            self.client.clone(),
            index,
            schema,
        )))
    }

    /// A table over `index` with a declared schema (no `_mapping` round-trip; only the listed
    /// columns are read, and a not-yet-created index reads as empty).
    pub fn table_provider_with_schema(&self, index: impl Into<String>, schema: SchemaRef) -> Arc<dyn TableProvider> {
        Arc::new(OpenSearchTableProvider::with_client_and_schema(
            self.client.clone(),
            index,
            schema,
        ))
    }

    /// The provider (not type-erased), for callers that want the builder methods.
    pub fn provider_with_schema(&self, index: impl Into<String>, schema: SchemaRef) -> OpenSearchTableProvider {
        OpenSearchTableProvider::with_client_and_schema(self.client.clone(), index, schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("status", DataType::Utf8, true),
            Field::new("speed", DataType::Float64, true),
            Field::new("active", DataType::Boolean, true),
        ]))
    }

    #[test]
    fn projected_schema_selects_a_subset() {
        let proj = schema().project(&[0, 1]).unwrap();
        assert_eq!(proj.fields().len(), 2);
        assert_eq!(proj.field(0).name(), "id");
        assert_eq!(proj.field(1).name(), "status");
    }

    #[test]
    fn with_base_filter_is_recorded() {
        let p = OpenSearchTableProvider::new("http://x", "i", schema())
            .with_base_filter(vec![json!({ "term": { "tenant": "t1" } })]);
        assert_eq!(p.base_filter.len(), 1);
        assert_eq!(p.index(), "i");
    }

    #[test]
    fn factory_builds_a_declared_schema_provider_without_io() {
        let f = OpenSearchTableFactory::new(OpenSearchClient::new("http://x:9200"));
        let p = f.provider_with_schema("idx", schema()).with_default_size(5);
        assert_eq!(p.default_size, 5);
        assert_eq!(f.table_provider_with_schema("idx", schema()).schema().fields().len(), 4);
    }
}
