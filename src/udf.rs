//! OpenSearch-native predicates as DataFusion scalar UDFs (feature `udf`).
//!
//! Some of the most useful OpenSearch queries have no SQL spelling — an analyzed full-text `match`,
//! or a geo bounding-box / distance / polygon filter. This module exposes them as Boolean scalar
//! UDFs so they travel through the planner like any other predicate, and
//! [`pushdown::expr_to_query`](crate::pushdown) recognises them **by name** and emits the native
//! query. The provider reports such filters `Exact`, so DataFusion drops its in-memory copy.
//!
//! Register them on a `SessionContext` (or embed them in an `Expr` via `.call([...])`):
//!
//! ```
//! use datafusion::prelude::SessionContext;
//! let ctx = SessionContext::new();
//! ctx.register_udf(datafusion_opensearch::os_match_udf().as_ref().clone());
//! ctx.register_udf(datafusion_opensearch::os_geo_distance_udf().as_ref().clone());
//! // SELECT id FROM docs WHERE os_match(title, 'engine fault') AND os_geo_distance(pos, 24.9, 60.2, 5000)
//! ```
//!
//! Because pushdown keys off the registered name, the names below are part of the contract. In a
//! distributed setting (e.g. Ballista) every node that plans or executes must register the same
//! UDFs, since plans serialise scalar functions by name.

use std::sync::{Arc, OnceLock};

use datafusion::arrow::array::{Array, BooleanArray, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility};
use datafusion::scalar::ScalarValue;

/// `os_match(field, query)` — full-text `match`. The registered name pushdown keys off.
pub const OS_MATCH: &str = "os_match";
/// `os_geo_bbox(field, top_left_lon, top_left_lat, bottom_right_lon, bottom_right_lat)` → `geo_bounding_box`.
pub const OS_GEO_BBOX: &str = "os_geo_bbox";
/// `os_geo_distance(field, center_lon, center_lat, radius_metres)` → `geo_distance`.
pub const OS_GEO_DISTANCE: &str = "os_geo_distance";
/// `os_geo_polygon(field, '<json [[lon,lat],…]>')` → `geo_polygon`.
pub const OS_GEO_POLYGON: &str = "os_geo_polygon";

/// Tokenise text the way the in-memory `os_match` fallback compares it: split on non-alphanumeric
/// characters, lowercase, drop empties. Deliberately simple — the pushed-down `match` query uses
/// the index's own analyzer; this only governs the fallback when a filter is evaluated in memory.
pub fn tokens(s: &str) -> Vec<String> {
    s.split(|c: char| !c.is_alphanumeric())
        .filter(|t| !t.is_empty())
        .map(str::to_lowercase)
        .collect()
}

/// OR token semantics: any query token present among the document's tokens (empty query → no match).
fn matches(doc: &str, query: &str) -> bool {
    let q = tokens(query);
    !q.is_empty() && {
        let d = tokens(doc);
        q.iter().any(|t| d.contains(t))
    }
}

/// The `os_match` UDF: `(Utf8 field, Utf8 query) -> Boolean`.
#[derive(Debug, PartialEq, Eq, Hash)]
struct MatchUdf {
    signature: Signature,
}

impl MatchUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Utf8, DataType::Utf8], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for MatchUdf {
    fn name(&self) -> &str {
        OS_MATCH
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }
    /// The in-memory fallback, used only if a `match` filter is evaluated by DataFusion rather
    /// than pushed down: tokenised OR match against a constant query. A non-literal query → false.
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        let query = match args.args.get(1) {
            Some(ColumnarValue::Scalar(ScalarValue::Utf8(Some(q)))) => q.clone(),
            _ => return Ok(ColumnarValue::Array(Arc::new(BooleanArray::from(vec![false; n])))),
        };
        let col = args.args[0].clone().into_array(n)?;
        let out: BooleanArray = match col.as_any().downcast_ref::<StringArray>() {
            Some(sa) => (0..sa.len())
                .map(|i| Some(!sa.is_null(i) && matches(sa.value(i), &query)))
                .collect(),
            None => BooleanArray::from(vec![false; n]),
        };
        Ok(ColumnarValue::Array(Arc::new(out)))
    }
}

/// The shared `os_match` UDF (built once).
pub fn os_match_udf() -> Arc<ScalarUDF> {
    static UDF: OnceLock<Arc<ScalarUDF>> = OnceLock::new();
    UDF.get_or_init(|| Arc::new(ScalarUDF::from(MatchUdf::new()))).clone()
}

/// A geo predicate UDF — a Boolean function whose ONLY purpose is to carry a geo query and its
/// literal bounds through the planner to pushdown, which emits the native OpenSearch query. Geo
/// filters are always pushed (`Exact`), so the in-memory `invoke` is a fail-CLOSED stub (returns
/// false — never widens the result) rather than a full geo evaluation.
#[derive(Debug, PartialEq, Eq, Hash)]
struct GeoUdf {
    name: &'static str,
    signature: Signature,
}

impl ScalarUDFImpl for GeoUdf {
    fn name(&self) -> &str {
        self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(ColumnarValue::Array(Arc::new(BooleanArray::from(vec![
            false;
            args.number_rows
        ]))))
    }
}

fn geo_udf(name: &'static str, arity: usize) -> Arc<ScalarUDF> {
    // `any(arity)`: the args are a geo column + numeric literals; only pushdown reads them, so the
    // signature just needs to accept the call, not coerce types.
    Arc::new(ScalarUDF::from(GeoUdf {
        name,
        signature: Signature::any(arity, Volatility::Immutable),
    }))
}

/// `os_geo_bbox(field, tl_lon, tl_lat, br_lon, br_lat)` → OpenSearch `geo_bounding_box`.
pub fn os_geo_bbox_udf() -> Arc<ScalarUDF> {
    static UDF: OnceLock<Arc<ScalarUDF>> = OnceLock::new();
    UDF.get_or_init(|| geo_udf(OS_GEO_BBOX, 5)).clone()
}

/// `os_geo_distance(field, center_lon, center_lat, radius_metres)` → OpenSearch `geo_distance`.
pub fn os_geo_distance_udf() -> Arc<ScalarUDF> {
    static UDF: OnceLock<Arc<ScalarUDF>> = OnceLock::new();
    UDF.get_or_init(|| geo_udf(OS_GEO_DISTANCE, 4)).clone()
}

/// `os_geo_polygon(field, '<json [[lon,lat],…]>')` → OpenSearch `geo_polygon`. The ring rides ONE
/// JSON-string literal (a scalar UDF can't take a variable-length point list); only pushdown parses it.
pub fn os_geo_polygon_udf() -> Arc<ScalarUDF> {
    static UDF: OnceLock<Arc<ScalarUDF>> = OnceLock::new();
    UDF.get_or_init(|| geo_udf(OS_GEO_POLYGON, 2)).clone()
}

/// All four UDFs, for bulk registration: `for f in all_udfs() { ctx.register_udf(f.as_ref().clone()) }`.
pub fn all_udfs() -> [Arc<ScalarUDF>; 4] {
    [
        os_match_udf(),
        os_geo_bbox_udf(),
        os_geo_distance_udf(),
        os_geo_polygon_udf(),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokenise_splits_lowercases_and_drops_empties() {
        assert_eq!(tokens("Engine Overheating!"), vec!["engine", "overheating"]);
        assert_eq!(tokens("fault(sensor,7)"), vec!["fault", "sensor", "7"]);
        assert!(tokens("  ").is_empty());
    }

    #[test]
    fn or_token_match_is_case_insensitive_and_not_substring() {
        assert!(matches("engine overheating detected", "ENGINE")); // case-insensitive
        assert!(matches("brakes worn", "engine brakes")); // OR: any token suffices
        assert!(!matches("engine", "eng")); // substring is NOT a token match
        assert!(!matches("anything", "  ")); // empty query → false
    }

    #[test]
    fn udfs_are_named_and_boolean() {
        let udf = os_match_udf();
        assert_eq!(udf.name(), OS_MATCH);
        assert_eq!(
            udf.return_type(&[DataType::Utf8, DataType::Utf8]).unwrap(),
            DataType::Boolean
        );
        let names: Vec<String> = all_udfs().iter().map(|u| u.name().to_string()).collect();
        assert_eq!(names, [OS_MATCH, OS_GEO_BBOX, OS_GEO_DISTANCE, OS_GEO_POLYGON]);
    }
}
