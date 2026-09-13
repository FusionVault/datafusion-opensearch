//! A [`SchemaProvider`] that exposes a cluster's indices as tables, so `SHOW TABLES` lists them
//! and `SELECT … FROM <schema>.<index>` works without registering each index by hand.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use datafusion::catalog::{SchemaProvider, TableProvider};
use datafusion::error::{DataFusionError, Result as DataFusionResult};

use crate::client::OpenSearchClient;
use crate::table::OpenSearchTableFactory;
use crate::Result;

/// One OpenSearch cluster as a DataFusion schema: every non-system index is a table whose Arrow
/// schema is derived from its `_mapping` the first time it is referenced (and cached). `SHOW
/// TABLES` lists them when the session has `information_schema` enabled.
///
/// ```no_run
/// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
/// use std::sync::Arc;
/// use datafusion::prelude::SessionContext;
/// use datafusion_opensearch::{OpenSearchClient, OpenSearchSchemaProvider};
///
/// let ctx = SessionContext::new();
/// let os = OpenSearchSchemaProvider::connect(OpenSearchClient::new("http://localhost:9200")).await?;
/// ctx.catalog("datafusion").unwrap().register_schema("os", Arc::new(os))?;
/// ctx.sql("SELECT count(*) FROM os.\"my-index\"").await?.show().await?;
/// # Ok(()) }
/// ```
#[derive(Debug)]
pub struct OpenSearchSchemaProvider {
    factory: OpenSearchTableFactory,
    names: RwLock<Vec<String>>,
    tables: RwLock<HashMap<String, Arc<dyn TableProvider>>>,
}

impl OpenSearchSchemaProvider {
    /// List the cluster's indices now; tables are built lazily on first reference.
    pub async fn connect(client: OpenSearchClient) -> Result<Self> {
        let names = client.indices().await?;
        Ok(Self {
            factory: OpenSearchTableFactory::new(client),
            names: RwLock::new(names),
            tables: RwLock::new(HashMap::new()),
        })
    }

    /// Re-list the indices and drop cached tables for indices that no longer exist.
    pub async fn refresh(&self) -> Result<()> {
        let names = self.factory.client().indices().await?;
        self.tables.write().unwrap().retain(|k, _| names.contains(k));
        *self.names.write().unwrap() = names;
        Ok(())
    }

    /// The client the schema talks through.
    pub fn client(&self) -> &OpenSearchClient {
        self.factory.client()
    }
}

#[async_trait]
impl SchemaProvider for OpenSearchSchemaProvider {
    fn table_names(&self) -> Vec<String> {
        self.names.read().unwrap().clone()
    }

    async fn table(&self, name: &str) -> DataFusionResult<Option<Arc<dyn TableProvider>>> {
        if !self.table_exist(name) {
            return Ok(None);
        }
        if let Some(t) = self.tables.read().unwrap().get(name) {
            return Ok(Some(t.clone()));
        }
        let table = self
            .factory
            .table_provider(name)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        self.tables.write().unwrap().insert(name.to_string(), table.clone());
        Ok(Some(table))
    }

    fn table_exist(&self, name: &str) -> bool {
        self.names.read().unwrap().iter().any(|n| n == name)
    }
}
