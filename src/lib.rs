//! A DataFusion [`TableProvider`] over an OpenSearch (or Elasticsearch) index.
//!
//! `SELECT … FROM <index> WHERE …` planned by DataFusion becomes an OpenSearch `_search`: the
//! WHERE is pushed into the query DSL ([`pushdown::expr_to_query`]), the projection into
//! `_source`, and the LIMIT into `size`. Filters that cannot be translated exactly are reported
//! [`TableProviderFilterPushDown::Unsupported`], so DataFusion re-applies them in-memory — partial
//! pushdown is therefore always correct and never widens the result set.
//!
//! Two ways to build a table:
//!
//! - [`OpenSearchTableProvider::new`] with a declared Arrow schema (only the listed columns are read).
//! - [`OpenSearchTableFactory::table_provider`], which derives the schema from the index `_mapping`
//!   ([`schema::schema_from_mapping`]).
//!
//! ```no_run
//! use std::sync::Arc;
//! use datafusion::arrow::datatypes::{DataType, Field, Schema};
//! use datafusion::prelude::SessionContext;
//! use datafusion_opensearch::OpenSearchTableProvider;
//!
//! # async fn run() -> datafusion::error::Result<()> {
//! let schema = Arc::new(Schema::new(vec![
//!     Field::new("id", DataType::Utf8, true),
//!     Field::new("status", DataType::Utf8, true),
//! ]));
//! let ctx = SessionContext::new();
//! ctx.register_table(
//!     "docs",
//!     Arc::new(OpenSearchTableProvider::new("http://localhost:9200", "my-index", schema)),
//! )?;
//! let df = ctx.sql("SELECT id FROM docs WHERE status = 'OK' LIMIT 10").await?;
//! df.show().await?;
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

pub mod client;
pub mod pushdown;
pub mod schema;
pub mod table;
#[cfg(feature = "udf")]
pub mod udf;

pub use client::OpenSearchClient;
pub use schema::schema_from_mapping;
pub use table::{OpenSearchTableFactory, OpenSearchTableProvider, DEFAULT_SIZE};
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
