//! The HTTP client the provider talks to OpenSearch through.
//!
//! OpenSearch's REST API is stateless, so there is no connection pool to manage: a
//! [`reqwest::Client`] (which pools connections internally) plus the cluster's base URL is the
//! whole "connection". Add credentials with [`with_basic_auth`](OpenSearchClient::with_basic_auth)
//! or [`with_bearer_token`](OpenSearchClient::with_bearer_token), or bring your own client via
//! [`with_client`](OpenSearchClient::with_client) for timeouts, TLS roots and custom headers.

use serde_json::{json, Value};

use crate::{DecodeSnafu, Error, HttpSnafu, IndexNotFoundSnafu, Result};
use snafu::ResultExt;

#[derive(Clone, Debug)]
enum Auth {
    None,
    Basic { user: String, password: String },
    Bearer(String),
}

/// A handle to one OpenSearch cluster.
#[derive(Clone, Debug)]
pub struct OpenSearchClient {
    base_url: String,
    http: reqwest::Client,
    auth: Auth,
}

/// One page of search hits: the `_source` documents plus the scroll cursor, if one is open.
#[derive(Clone, Debug, Default)]
pub struct SearchPage {
    /// Each hit's `_source`.
    pub hits: Vec<Value>,
    /// The `_scroll_id` to continue with, when the search opened a scroll.
    pub scroll_id: Option<String>,
}

impl OpenSearchClient {
    /// `base_url` e.g. `http://localhost:9200` (a trailing slash is tolerated).
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            http: reqwest::Client::new(),
            auth: Auth::None,
        }
    }

    /// Use a preconfigured [`reqwest::Client`] (timeouts, connection limits, TLS roots, headers).
    pub fn with_client(mut self, http: reqwest::Client) -> Self {
        self.http = http;
        self
    }

    /// Send HTTP basic credentials with every request (the OpenSearch security plugin's default).
    pub fn with_basic_auth(mut self, user: impl Into<String>, password: impl Into<String>) -> Self {
        self.auth = Auth::Basic {
            user: user.into(),
            password: password.into(),
        };
        self
    }

    /// Send a bearer token with every request.
    pub fn with_bearer_token(mut self, token: impl Into<String>) -> Self {
        self.auth = Auth::Bearer(token.into());
        self
    }

    /// The cluster base URL, without a trailing slash.
    pub fn base_url(&self) -> &str {
        self.base_url.trim_end_matches('/')
    }

    fn get(&self, url: &str) -> reqwest::RequestBuilder {
        self.authed(self.http.get(url))
    }

    fn post(&self, url: &str) -> reqwest::RequestBuilder {
        self.authed(self.http.post(url))
    }

    fn delete(&self, url: &str) -> reqwest::RequestBuilder {
        self.authed(self.http.delete(url))
    }

    fn authed(&self, rb: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.auth {
            Auth::None => rb,
            Auth::Basic { user, password } => rb.basic_auth(user, Some(password)),
            Auth::Bearer(t) => rb.bearer_auth(t),
        }
    }

    /// Run one `_search` against `index` and return each hit's `_source`. A missing index (404)
    /// yields an empty result rather than an error, so a table over an index that has not been
    /// created yet simply reads as empty.
    pub async fn search(&self, index: &str, body: &Value) -> Result<Vec<Value>> {
        Ok(self.search_page(index, body, None).await?.hits)
    }

    /// Run a `_search`, optionally opening a scroll cursor (`keep_alive` e.g. `"1m"`) so the rest
    /// of the result can be paged with [`scroll`](Self::scroll). A missing index reads as empty.
    pub async fn search_page(&self, index: &str, body: &Value, keep_alive: Option<&str>) -> Result<SearchPage> {
        let url = match keep_alive {
            Some(k) => format!("{}/{index}/_search?scroll={k}", self.base_url()),
            None => format!("{}/{index}/_search", self.base_url()),
        };
        let resp = self
            .post(&url)
            .json(body)
            .send()
            .await
            .context(HttpSnafu { url: url.clone() })?;
        if resp.status().as_u16() == 404 {
            return Ok(SearchPage::default());
        }
        let resp = check_status(resp, &url).await?;
        let doc: Value = resp.json().await.context(DecodeSnafu { url })?;
        Ok(page_from(doc))
    }

    /// Fetch the next page of an open scroll.
    pub async fn scroll(&self, scroll_id: &str, keep_alive: &str) -> Result<SearchPage> {
        let url = format!("{}/_search/scroll", self.base_url());
        let resp = self
            .post(&url)
            .json(&json!({ "scroll": keep_alive, "scroll_id": scroll_id }))
            .send()
            .await
            .context(HttpSnafu { url: url.clone() })?;
        let resp = check_status(resp, &url).await?;
        let doc: Value = resp.json().await.context(DecodeSnafu { url })?;
        Ok(page_from(doc))
    }

    /// Release a scroll cursor early (best effort — the server expires it anyway).
    pub async fn clear_scroll(&self, scroll_id: &str) {
        let url = format!("{}/_search/scroll", self.base_url());
        let _ = self
            .delete(&url)
            .json(&json!({ "scroll_id": [scroll_id] }))
            .send()
            .await;
    }

    /// Fetch the index `_mapping` (the raw response, keyed by index name). A missing index is
    /// [`Error::IndexNotFound`] — unlike [`search`](Self::search), a caller asking for the schema of
    /// an index that does not exist has nothing sensible to fall back to.
    pub async fn mapping(&self, index: &str) -> Result<Value> {
        let url = format!("{}/{index}/_mapping", self.base_url());
        let resp = self.get(&url).send().await.context(HttpSnafu { url: url.clone() })?;
        if resp.status().as_u16() == 404 {
            return IndexNotFoundSnafu { index }.fail();
        }
        let resp = check_status(resp, &url).await?;
        resp.json().await.context(DecodeSnafu { url })
    }

    /// The names of the cluster's indices, sorted, excluding system indices (names starting with
    /// `.`). Aliases are not included.
    pub async fn indices(&self) -> Result<Vec<String>> {
        let url = format!("{}/_cat/indices?format=json&h=index", self.base_url());
        let resp = self.get(&url).send().await.context(HttpSnafu { url: url.clone() })?;
        let resp = check_status(resp, &url).await?;
        let doc: Value = resp.json().await.context(DecodeSnafu { url })?;
        let mut names: Vec<String> = doc
            .as_array()
            .map(|rows| {
                rows.iter()
                    .filter_map(|r| r["index"].as_str())
                    .filter(|n| !n.starts_with('.'))
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default();
        names.sort();
        Ok(names)
    }
}

fn page_from(doc: Value) -> SearchPage {
    let hits = doc["hits"]["hits"]
        .as_array()
        .map(|hits| hits.iter().map(|h| h["_source"].clone()).collect())
        .unwrap_or_default();
    let scroll_id = doc["_scroll_id"].as_str().map(str::to_string);
    SearchPage { hits, scroll_id }
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

    #[test]
    fn auth_is_recorded() {
        let c = OpenSearchClient::new("http://x").with_basic_auth("admin", "pw");
        assert!(matches!(c.auth, Auth::Basic { .. }));
        let c = OpenSearchClient::new("http://x").with_bearer_token("t");
        assert!(matches!(c.auth, Auth::Bearer(_)));
    }

    #[test]
    fn page_parses_hits_and_scroll_id() {
        let p = page_from(json!({ "_scroll_id": "abc", "hits": { "hits": [ { "_source": { "a": 1 } } ] } }));
        assert_eq!(p.hits, vec![json!({ "a": 1 })]);
        assert_eq!(p.scroll_id.as_deref(), Some("abc"));
        let empty = page_from(json!({ "hits": { "hits": [] } }));
        assert!(empty.hits.is_empty() && empty.scroll_id.is_none());
    }
}
