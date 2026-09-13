//! A DataFusion [`TableProvider`] over an OpenSearch (or Elasticsearch) index.
//!
//! `SELECT … FROM <index> WHERE …` planned by DataFusion becomes an OpenSearch `_search`: the
//! WHERE is pushed into the query DSL ([`pushdown::expr_to_query`]), the projection into
//! `_source`, and the LIMIT into the page size. The result is **streamed page by page** through a
//! scroll cursor ([`OpenSearchExec`]), so a scan is complete at any index size with memory bounded
//! by the page size. Filters that cannot be translated exactly are reported
//! [`TableProviderFilterPushDown::Unsupported`], so DataFusion re-applies them in-memory — partial
//! pushdown is therefore always correct and never widens the result set.
//!
//! Three ways in:
//!
//! - SQL only: register [`OpenSearchTableProviderFactory`] and
//!   `CREATE EXTERNAL TABLE docs STORED AS OPENSEARCH LOCATION 'http://localhost:9200/my-index'`.
//! - A whole cluster: [`OpenSearchSchemaProvider`] lists every index as a table (`SHOW TABLES`).
//! - One index: [`OpenSearchTableFactory::table_provider`] (schema from the `_mapping`) or
//!   [`OpenSearchTableProvider::new`] with a declared Arrow schema.
//!
//! ```no_run
//! use datafusion::prelude::SessionContext;
//! use datafusion_opensearch::OpenSearchTableProviderFactory;
//!
//! # async fn run() -> datafusion::error::Result<()> {
//! let ctx = SessionContext::new();
//! OpenSearchTableProviderFactory::new().register(&ctx);
//! ctx.sql("CREATE EXTERNAL TABLE docs STORED AS OPENSEARCH LOCATION 'http://localhost:9200/my-index'")
//!     .await?
//!     .collect()
//!     .await?;
//! ctx.sql("SELECT id, status FROM docs WHERE speed > 40 AND status = 'OK' LIMIT 100").await?.show().await?;
//! # Ok(())
//! # }
//! ```
//!
//! With the `udf` feature, OpenSearch-native predicates — full-text `match` and the geo bounding
//! box / distance / polygon queries — are available as scalar UDFs ([`udf`]) that the provider
//! pushes down as native queries.
//!
//! [`TableProvider`]: datafusion::catalog::TableProvider
//! [`TableProviderFilterPushDown::Unsupported`]: datafusion::logical_expr::TableProviderFilterPushDown::Unsupported

/// Every Rust code block in the README is compiled as a doctest (`no_run`: they need a server).
#[cfg(doctest)]
#[doc = include_str!("../README.md")]
pub struct ReadmeDoctests;

pub mod catalog;
pub mod client;
pub mod exec;
pub mod pushdown;
pub mod schema;
pub mod table;
#[cfg(feature = "udf")]
pub mod udf;

pub use catalog::OpenSearchSchemaProvider;
pub use client::{OpenSearchClient, SearchPage};
pub use exec::OpenSearchExec;
pub use schema::schema_from_mapping;
pub use table::{
    OpenSearchTableFactory, OpenSearchTableProvider, OpenSearchTableProviderFactory, DEFAULT_PAGE_SIZE, FILE_TYPE,
};
#[cfg(feature = "udf")]
pub use udf::{
    os_geo_bbox_udf, os_geo_distance_udf, os_geo_polygon_udf, os_match_udf, OS_GEO_BBOX, OS_GEO_DISTANCE,
    OS_GEO_POLYGON, OS_MATCH,
};

use datafusion::error::DataFusionError;
use snafu::Snafu;

/// Errors raised while talking to OpenSearch or shaping its responses. Inside a DataFusion plan
/// they surface as [`DataFusionError::External`].
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("request to {url} failed: {source}"))]
    Http { url: String, source: reqwest::Error },

    #[snafu(display("OpenSearch returned {status} for {url}: {body}"))]
    Status { status: u16, url: String, body: String },

    #[snafu(display("index '{index}' does not exist"))]
    IndexNotFound { index: String },

    #[snafu(display("could not decode the OpenSearch response from {url}: {source}"))]
    Decode { url: String, source: reqwest::Error },

    #[snafu(display("the _mapping response for '{index}' has no 'mappings.properties'"))]
    MalformedMapping { index: String },
}

/// Convenience alias used throughout the crate.
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl From<Error> for DataFusionError {
    fn from(e: Error) -> Self {
        DataFusionError::External(Box::new(e))
    }
}
