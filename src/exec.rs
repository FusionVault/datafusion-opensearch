//! The physical operator: a `_search` that streams its result page by page.
//!
//! One `_search` can return at most `index.max_result_window` hits (10 000 by default), so a scan
//! that simply set `size` would silently truncate any larger result. [`OpenSearchExec`] instead
//! opens a scroll cursor and emits one `RecordBatch` per page until the result is exhausted or the
//! plan's `fetch` (a pushed-down `LIMIT`) is satisfied, so memory stays bounded by the page size
//! and the result is complete. With more than one partition the scan is a **sliced scroll**: each
//! DataFusion partition reads one slice (`slice: { id, max }`) in parallel, which is how providers
//! over sharded stores fan out. A `LIMIT` or `search_after` read always uses one partition, so a
//! limit is exact.

use std::fmt;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties, SendableRecordBatchStream,
};
use futures::StreamExt;
use serde_json::Value;

use crate::client::OpenSearchClient;
use crate::schema::build_batch;

/// How long the server keeps a scroll cursor alive between pages.
const KEEP_ALIVE: &str = "1m";

/// A scan of one index, streamed as pages of `_source` documents turned into record batches.
#[derive(Debug)]
pub struct OpenSearchExec {
    inner: Arc<Inner>,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

#[derive(Clone, Debug)]
struct Inner {
    client: OpenSearchClient,
    index: String,
    /// The `_search` body without `size`: query, `_source`, sort, search_after.
    body: Value,
    schema: SchemaRef,
    page_size: usize,
    /// Stop after this many rows (a pushed-down LIMIT or the table's row cap).
    fetch: Option<usize>,
    /// `search_after` pagination is the caller's loop: read exactly one page.
    single_page: bool,
    /// Sliced-scroll partitions (1 = a plain scroll).
    partitions: usize,
}

impl OpenSearchExec {
    /// A scan of `index` sending `body` (the `_search` request without `size`), producing
    /// `schema`. Tune it with the `with_*` builders; by default it streams the whole result
    /// through one scroll in pages of 5 000.
    pub fn new(client: OpenSearchClient, index: String, body: Value, schema: SchemaRef) -> Self {
        let inner = Inner {
            client,
            index,
            body,
            schema,
            page_size: crate::table::DEFAULT_PAGE_SIZE,
            fetch: None,
            single_page: false,
            partitions: 1,
        };
        Self::from_inner(inner)
    }

    fn from_inner(inner: Inner) -> Self {
        // A fetch or a single-page read needs one stream so the row cap stays exact.
        let partitions = if inner.fetch.is_some() || inner.single_page {
            1
        } else {
            inner.partitions.max(1)
        };
        let inner = Inner { partitions, ..inner };
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(inner.schema.clone()),
            Partitioning::UnknownPartitioning(partitions),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            inner: Arc::new(inner),
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    fn rebuild(&self, f: impl FnOnce(&mut Inner)) -> Self {
        let mut inner = self.inner.as_ref().clone();
        f(&mut inner);
        Self::from_inner(inner)
    }

    /// Hits per page (at least 1).
    pub fn with_page_size(&self, n: usize) -> Self {
        self.rebuild(|i| i.page_size = n.max(1))
    }

    /// Stop after this many rows (a pushed-down LIMIT or the table's row cap). Forces one partition.
    pub fn with_fetch_limit(&self, fetch: Option<usize>) -> Self {
        self.rebuild(|i| i.fetch = fetch)
    }

    /// Read exactly one page (caller-driven `search_after` pagination). Forces one partition.
    pub fn with_single_page(&self, single: bool) -> Self {
        self.rebuild(|i| i.single_page = single)
    }

    /// Fan the scan out over `n` sliced scrolls, one per DataFusion partition.
    pub fn with_partitions(&self, n: usize) -> Self {
        self.rebuild(|i| i.partitions = n.max(1))
    }

    /// The index being scanned.
    pub fn index(&self) -> &str {
        &self.inner.index
    }

    /// The `_search` body (without `size`) this scan sends — useful to inspect what was pushed down.
    pub fn body(&self) -> &Value {
        &self.inner.body
    }
}

impl DisplayAs for OpenSearchExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let i = &self.inner;
        match t {
            DisplayFormatType::Default | DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "OpenSearchExec: index={}, partitions={}, fetch={:?}",
                    i.index, i.partitions, i.fetch
                )
            }
            DisplayFormatType::Verbose => write!(
                f,
                "OpenSearchExec: index={}, partitions={}, fetch={:?}, page_size={}, body={}",
                i.index, i.partitions, i.fetch, i.page_size, i.body
            ),
        }
    }
}

struct PageState {
    inner: Arc<Inner>,
    partition: usize,
    scroll_id: Option<String>,
    remaining: Option<usize>,
    started: bool,
    done: bool,
}

async fn next_page(mut st: PageState) -> Result<Option<(RecordBatch, PageState)>> {
    if st.done || st.remaining == Some(0) {
        if let Some(id) = st.scroll_id.take() {
            st.inner.client.clear_scroll(&id).await;
        }
        return Ok(None);
    }
    let size = st.remaining.map_or(st.inner.page_size, |r| r.min(st.inner.page_size));
    let page = if !st.started {
        st.started = true;
        let mut body = st.inner.body.clone();
        body["size"] = Value::from(size);
        if st.inner.partitions > 1 {
            body["slice"] = serde_json::json!({ "id": st.partition, "max": st.inner.partitions });
        }
        let keep_alive = if st.inner.single_page { None } else { Some(KEEP_ALIVE) };
        st.inner.client.search_page(&st.inner.index, &body, keep_alive).await?
    } else {
        match &st.scroll_id {
            Some(id) => st.inner.client.scroll(id, KEEP_ALIVE).await?,
            None => return Ok(None),
        }
    };
    if page.scroll_id.is_some() {
        st.scroll_id = page.scroll_id;
    }
    if page.hits.is_empty() {
        st.done = true;
        if let Some(id) = st.scroll_id.take() {
            st.inner.client.clear_scroll(&id).await;
        }
        return Ok(None);
    }
    // A scroll keeps the first request's page size, so the last page can overshoot the fetch:
    // keep only what is still wanted.
    let hits = truncate_to_fetch(page.hits, st.remaining);
    if let Some(r) = st.remaining.as_mut() {
        *r = r.saturating_sub(hits.len());
    }
    // A short page, a single-page read, or an exhausted fetch ends the scan after this batch.
    if st.inner.single_page || hits.len() < size || st.remaining == Some(0) {
        st.done = true;
    }
    let batch = build_batch(&st.inner.schema, &hits)?;
    Ok(Some((batch, st)))
}

/// Drop hits beyond the rows still wanted (`None` = unlimited).
fn truncate_to_fetch(mut hits: Vec<Value>, remaining: Option<usize>) -> Vec<Value> {
    if let Some(r) = remaining {
        hits.truncate(r);
    }
    hits
}

impl ExecutionPlan for OpenSearchExec {
    fn name(&self) -> &str {
        "OpenSearchExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(self: Arc<Self>, _children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(&self, partition: usize, _context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        if partition >= self.inner.partitions {
            return Err(datafusion::error::DataFusionError::Internal(format!(
                "OpenSearchExec: partition {partition} requested of {}",
                self.inner.partitions
            )));
        }
        let state = PageState {
            inner: self.inner.clone(),
            partition,
            scroll_id: None,
            remaining: self.inner.fetch,
            started: false,
            done: false,
        };
        let baseline = BaselineMetrics::new(&self.metrics, partition);
        let stream = futures::stream::try_unfold(state, next_page)
            .map(move |r| {
                if let Ok(batch) = &r {
                    baseline.record_output(batch.num_rows());
                }
                r
            })
            .boxed();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.inner.schema.clone(),
            stream,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn supports_limit_pushdown(&self) -> bool {
        true
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        let fetch = match (self.inner.fetch, limit) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        Some(Arc::new(self.with_fetch_limit(fetch)))
    }

    fn fetch(&self) -> Option<usize> {
        self.inner.fetch
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use serde_json::json;

    fn exec(fetch: Option<usize>) -> OpenSearchExec {
        partitioned(fetch, 1)
    }

    fn partitioned(fetch: Option<usize>, partitions: usize) -> OpenSearchExec {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, true)]));
        OpenSearchExec::new(
            OpenSearchClient::new("http://x:9200"),
            "idx".into(),
            json!({ "query": { "match_all": {} } }),
            schema,
        )
        .with_page_size(500)
        .with_fetch_limit(fetch)
        .with_partitions(partitions)
    }

    #[test]
    fn properties_single_bounded_incremental_partition() {
        let e = exec(None);
        assert_eq!(e.properties().partitioning.partition_count(), 1);
        assert_eq!(e.properties().emission_type, EmissionType::Incremental);
        assert_eq!(e.properties().boundedness, Boundedness::Bounded);
        assert_eq!(e.schema().fields().len(), 1);
        assert!(e.children().is_empty());
    }

    #[test]
    fn fetch_narrows_but_never_widens() {
        let e = exec(Some(100));
        assert_eq!(e.fetch(), Some(100));
        assert!(e.supports_limit_pushdown());
        let tighter = e.with_fetch(Some(10)).unwrap();
        assert_eq!(tighter.fetch(), Some(10));
        let looser = e.with_fetch(Some(1000)).unwrap();
        assert_eq!(looser.fetch(), Some(100));
        assert_eq!(exec(None).with_fetch(Some(7)).unwrap().fetch(), Some(7));
    }

    #[test]
    fn the_last_page_is_truncated_to_the_fetch() {
        let hits: Vec<Value> = (0..5).map(|i| json!({ "n": i })).collect();
        assert_eq!(truncate_to_fetch(hits.clone(), Some(2)).len(), 2);
        assert_eq!(truncate_to_fetch(hits.clone(), Some(9)).len(), 5);
        assert_eq!(truncate_to_fetch(hits.clone(), None).len(), 5);
        assert!(truncate_to_fetch(hits, Some(0)).is_empty());
    }

    #[test]
    fn display_names_the_index() {
        let e = exec(Some(3));
        let s = datafusion::physical_plan::displayable(&e).one_line().to_string();
        assert!(
            s.contains("OpenSearchExec: index=idx, partitions=1, fetch=Some(3)"),
            "{s}"
        );
        assert_eq!(e.index(), "idx");
        assert_eq!(e.body()["query"], json!({ "match_all": {} }));
    }
}
