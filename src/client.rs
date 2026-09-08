//! The HTTP client the provider talks to OpenSearch through.
//!
//! OpenSearch's REST API is stateless, so there is no connection pool to manage: a
//! [`reqwest::Client`] (which pools connections internally) plus the cluster's base URL is the
//! whole "connection". Bring your own client via [`OpenSearchClient::with_client`] to set auth
//! headers, timeouts, or TLS roots.

use serde_json::Value;

use crate::{DecodeSnafu, Error, HttpSnafu, IndexNotFoundSnafu, Result};
use snafu::ResultExt;

/// A handle to one OpenSearch cluster.
#[derive(Clone, Debug)]
pub struct OpenSearchClient {
    base_url: String,
    http: reqwest::Client,
}

impl OpenSearchClient {
    /// `base_url` e.g. `http://localhost:9200` (a trailing slash is tolerated).
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            http: reqwest::Client::new(),
        }
    }

    /// Use a preconfigured [`reqwest::Client`] (auth headers, timeouts, connection limits, TLS roots).
    pub fn with_client(mut self, http: reqwest::Client) -> Self {
        self.http = http;
        self
    }

    /// The cluster base URL, without a trailing slash.
    pub fn base_url(&self) -> &str {
        self.base_url.trim_end_matches('/')
    }

    /// Run a `_search` against `index` and return each hit's `_source`. A missing index (404)
    /// yields an empty result rather than an error, so a table over an index that has not been
    /// created yet simply reads as empty.
    pub async fn search(&self, index: &str, body: &Value) -> Result<Vec<Value>> {
        let url = format!("{}/{index}/_search", self.base_url());
        let resp = self
            .http
            .post(&url)
            .json(body)
            .send()
            .await
            .context(HttpSnafu { url: url.clone() })?;
        if resp.status().as_u16() == 404 {
            return Ok(Vec::new());
        }
        let resp = check_status(resp, &url).await?;
        let doc: Value = resp.json().await.context(DecodeSnafu { url })?;
        Ok(doc["hits"]["hits"]
            .as_array()
            .map(|hits| hits.iter().map(|h| h["_source"].clone()).collect())
            .unwrap_or_default())
    }

    /// Fetch the index `_mapping` (the raw response, keyed by index name). A missing index is
    /// [`Error::IndexNotFound`] — unlike [`search`](Self::search), a caller asking for the schema of
    /// an index that does not exist has nothing sensible to fall back to.
    pub async fn mapping(&self, index: &str) -> Result<Value> {
        let url = format!("{}/{index}/_mapping", self.base_url());
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .context(HttpSnafu { url: url.clone() })?;
        if resp.status().as_u16() == 404 {
            return IndexNotFoundSnafu { index }.fail();
        }
        let resp = check_status(resp, &url).await?;
        resp.json().await.context(DecodeSnafu { url })
    }
}

/// Turn a non-2xx response into [`Error::Status`], carrying the body for diagnosis.
async fn check_status(resp: reqwest::Response, url: &str) -> Result<reqwest::Response> {
    if resp.status().is_success() {
        return Ok(resp);
    }
    let status = resp.status().as_u16();
    let body = resp.text().await.unwrap_or_default();
    Err(Error::Status {
        status,
        url: url.to_string(),
        body,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base_url_drops_a_trailing_slash() {
        assert_eq!(OpenSearchClient::new("http://x:9200/").base_url(), "http://x:9200");
        assert_eq!(OpenSearchClient::new("http://x:9200").base_url(), "http://x:9200");
    }
}
