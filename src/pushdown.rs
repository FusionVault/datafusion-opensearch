//! DataFusion `Expr` → OpenSearch query-DSL translation.
//!
//! The safety contract: [`expr_to_query`] returns `Some(json)` ONLY when it can translate the
//! filter EXACTLY (the OpenSearch query matches the same rows the `Expr` would). Anything it
//! cannot fully represent returns `None`, and the provider reports that filter as `Unsupported`
//! so DataFusion re-applies it in-memory — partial pushdown is therefore always correct, never a
//! silent widening.

use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use datafusion::scalar::ScalarValue;
use serde_json::{json, Value};

#[cfg(feature = "udf")]
use crate::udf::{OS_GEO_BBOX, OS_GEO_DISTANCE, OS_GEO_POLYGON, OS_MATCH};

/// A leaf column reference → its field name (only plain columns are pushable; a cast/function on a
/// column is not something OpenSearch can filter identically, so those bail to `None`).
fn column_name(e: &Expr) -> Option<&str> {
    match e {
        Expr::Column(c) => Some(c.name.as_str()),
        _ => None,
    }
}

/// A literal → JSON, for the scalar types documents actually carry. Returns `None` for NULLs
/// (a `= NULL` etc. has three-valued-logic semantics OpenSearch term queries don't reproduce) and
/// for types we don't model, so the filter falls back to DataFusion.
fn literal_json(e: &Expr) -> Option<Value> {
    let Expr::Literal(sv, _) = e else { return None };
    scalar_to_json(sv)
}

fn scalar_to_json(sv: &ScalarValue) -> Option<Value> {
    match sv {
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) | ScalarValue::Utf8View(Some(s)) => Some(json!(s)),
        ScalarValue::Boolean(Some(b)) => Some(json!(b)),
        ScalarValue::Int8(Some(n)) => Some(json!(n)),
        ScalarValue::Int16(Some(n)) => Some(json!(n)),
        ScalarValue::Int32(Some(n)) => Some(json!(n)),
        ScalarValue::Int64(Some(n)) => Some(json!(n)),
        ScalarValue::UInt8(Some(n)) => Some(json!(n)),
        ScalarValue::UInt16(Some(n)) => Some(json!(n)),
        ScalarValue::UInt32(Some(n)) => Some(json!(n)),
        ScalarValue::UInt64(Some(n)) => Some(json!(n)),
        ScalarValue::Float32(Some(f)) => Some(json!(f)),
        ScalarValue::Float64(Some(f)) => Some(json!(f)),
        // Temporal literals → epoch milliseconds, which OpenSearch's default `date` format
        // (`strict_date_optional_time||epoch_millis`) accepts in `term` and `range` queries.
        ScalarValue::TimestampSecond(Some(t), _) => Some(json!(t.checked_mul(1_000)?)),
        ScalarValue::TimestampMillisecond(Some(t), _) => Some(json!(t)),
        ScalarValue::TimestampMicrosecond(Some(t), _) => Some(json!(t.div_euclid(1_000))),
        ScalarValue::TimestampNanosecond(Some(t), _) => Some(json!(t.div_euclid(1_000_000))),
        ScalarValue::Date32(Some(d)) => Some(json!(i64::from(*d) * 86_400_000)),
        _ => None, // NULLs, decimal/nested types → let DataFusion handle it
    }
}

/// The `range` bound key for a comparison operator, if it is one.
fn range_key(op: &Operator) -> Option<&'static str> {
    match op {
        Operator::Gt => Some("gt"),
        Operator::GtEq => Some("gte"),
        Operator::Lt => Some("lt"),
        Operator::LtEq => Some("lte"),
        _ => None,
    }
}

/// Translate an `Expr` into an OpenSearch query clause, or `None` if it can't be represented exactly.
pub fn expr_to_query(expr: &Expr) -> Option<Value> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => match op {
            Operator::Eq => {
                let (col, lit) = col_lit(left, right)?;
                Some(json!({ "term": { col: lit } }))
            }
            Operator::NotEq => {
                let (col, lit) = col_lit(left, right)?;
                Some(json!({ "bool": { "must_not": { "term": { col: lit } } } }))
            }
            Operator::Gt | Operator::GtEq | Operator::Lt | Operator::LtEq => {
                // Normalize `lit OP col` into `col FLIPPED-OP lit` so both orderings push down.
                if let (Some(col), Some(lit)) = (column_name(left), literal_json(right)) {
                    Some(json!({ "range": { col: { range_key(op).unwrap(): lit } } }))
                } else if let (Some(col), Some(lit)) = (column_name(right), literal_json(left)) {
                    let flipped = match op {
                        Operator::Gt => "lt",
                        Operator::GtEq => "lte",
                        Operator::Lt => "gt",
                        Operator::LtEq => "gte",
                        _ => unreachable!(),
                    };
                    Some(json!({ "range": { col: { flipped: lit } } }))
                } else {
                    None
                }
            }
            Operator::And => {
                // AND pushes down if BOTH sides do (else the whole thing is Unsupported and
                // DataFusion applies it — safe). Note DataFusion already splits top-level ANDs
                // into separate filters, so this mainly matters for nested ANDs.
                Some(json!({ "bool": { "filter": [expr_to_query(left)?, expr_to_query(right)?] } }))
            }
            Operator::Or => {
                // OR is all-or-nothing: a partially-pushed OR would WIDEN the result. Both sides
                // must translate, else None.
                Some(
                    json!({ "bool": { "should": [expr_to_query(left)?, expr_to_query(right)?], "minimum_should_match": 1 } }),
                )
            }
            _ => None,
        },
        Expr::InList(il) => {
            let col = column_name(&il.expr)?;
            let mut values = Vec::with_capacity(il.list.len());
            for item in &il.list {
                values.push(literal_json(item)?); // any non-literal member → bail (Unsupported)
            }
            let terms = json!({ "terms": { col: values } });
            if il.negated {
                Some(json!({ "bool": { "must_not": terms } }))
            } else {
                Some(terms)
            }
        }
        Expr::Between(b) => {
            let col = column_name(&b.expr)?;
            let (lo, hi) = (literal_json(&b.low)?, literal_json(&b.high)?);
            let range = json!({ "range": { col: { "gte": lo, "lte": hi } } });
            if b.negated {
                Some(json!({ "bool": { "must_not": range } }))
            } else {
                Some(range)
            }
        }
        // `col IS NOT NULL` → exists; `col IS NULL` → must_not exists.
        Expr::IsNotNull(inner) => column_name(inner).map(|c| json!({ "exists": { "field": c } })),
        Expr::IsNull(inner) => {
            column_name(inner).map(|c| json!({ "bool": { "must_not": { "exists": { "field": c } } } }))
        }
        Expr::Not(inner) => Some(json!({ "bool": { "must_not": expr_to_query(inner)? } })),
        // SQL LIKE → an index-backed `prefix` query for the pure trailing-`%` case, else a
        // `wildcard` query. Pushing this (rather than an in-memory scan) is what keeps as-you-type
        // search scalable at any index size.
        Expr::Like(like) if like.escape_char.is_none() || like.escape_char == Some('\\') => {
            let col = column_name(&like.expr)?;
            let Expr::Literal(sv, _) = like.pattern.as_ref() else {
                return None;
            };
            let pattern = match scalar_to_json(sv)? {
                Value::String(s) => s,
                _ => return None,
            };
            let clause = like_to_query(col, &pattern, like.case_insensitive);
            if like.negated {
                Some(json!({ "bool": { "must_not": clause } }))
            } else {
                Some(clause)
            }
        }
        // Full-text `os_match(col, 'query')` → an OpenSearch analyzed `match` query (see
        // [`crate::udf`]). Represented as a UDF so it flows through the same pushdown as every
        // other predicate; the inverted index makes it scalable and fast.
        #[cfg(feature = "udf")]
        Expr::ScalarFunction(sf) if sf.func.name() == OS_MATCH => match sf.args.as_slice() {
            // os_match(<column>, '<query literal>') → { match: { field: query } }.
            [field, query] => match (column_name(field), literal_json(query)) {
                (Some(col), Some(Value::String(q))) => Some(json!({ "match": { col: q } })),
                _ => None,
            },
            _ => None,
        },
        // Geo predicates ([`crate::udf`]) → native OpenSearch geo queries. Corners/centres are [lon, lat].
        #[cfg(feature = "udf")]
        Expr::ScalarFunction(sf) if sf.func.name() == OS_GEO_BBOX => match sf.args.as_slice() {
            [field, tl_lon, tl_lat, br_lon, br_lat] => {
                let col = column_name(field)?;
                let (tll, tla, brl, bra) = (num(tl_lon)?, num(tl_lat)?, num(br_lon)?, num(br_lat)?);
                Some(json!({ "geo_bounding_box": { col: {
                    "top_left": { "lat": tla, "lon": tll },
                    "bottom_right": { "lat": bra, "lon": brl },
                } } }))
            }
            _ => None,
        },
        #[cfg(feature = "udf")]
        Expr::ScalarFunction(sf) if sf.func.name() == OS_GEO_POLYGON => match sf.args.as_slice() {
            [field, points_json] => {
                let col = column_name(field)?;
                let Value::String(raw) = literal_json(points_json)? else {
                    return None;
                };
                let pts: Vec<[f64; 2]> = serde_json::from_str(&raw).ok()?;
                if pts.len() < 3 {
                    return None; // not a polygon — keep the filter in DataFusion (fail-closed stub)
                }
                let points: Vec<Value> = pts.iter().map(|[lon, lat]| json!({ "lat": lat, "lon": lon })).collect();
                Some(json!({ "geo_polygon": { col: { "points": points } } }))
            }
            _ => None,
        },
        #[cfg(feature = "udf")]
        Expr::ScalarFunction(sf) if sf.func.name() == OS_GEO_DISTANCE => match sf.args.as_slice() {
            [field, lon, lat, radius_m] => {
                let col = column_name(field)?;
                let (lo, la, r) = (num(lon)?, num(lat)?, num(radius_m)?);
                Some(json!({ "geo_distance": { "distance": format!("{r}m"), col: { "lat": la, "lon": lo } } }))
            }
            _ => None,
        },
        _ => None,
    }
}

/// A numeric literal Expr → f64 (geo bounds arrive as int or float literals).
#[cfg(feature = "udf")]
fn num(e: &Expr) -> Option<f64> {
    match literal_json(e)? {
        Value::Number(n) => n.as_f64(),
        _ => None,
    }
}

/// Translate a SQL `LIKE` pattern into an OpenSearch clause. `%` = any run, `_` = any single char,
/// `\` escapes the next char. A pattern whose only wildcard is a trailing `%` becomes an index-backed
/// `prefix` query; anything else becomes a `wildcard` query (`%`→`*`, `_`→`?`, literal `*?\`
/// escaped). `case_insensitive` maps to OpenSearch's own flag.
fn like_to_query(col: &str, pattern: &str, case_insensitive: bool) -> Value {
    let mut prefix = String::new(); // literal text before the first wildcard
    let mut wildcard = String::new(); // full pattern translated to OpenSearch wildcard syntax
    let mut seen_wild = false;
    let mut pure_prefix = true; // only wildcard is a single trailing `%`
    let mut chars = pattern.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\\' => {
                if let Some(n) = chars.next() {
                    if !seen_wild {
                        prefix.push(n);
                    }
                    push_escaped(&mut wildcard, n);
                }
            }
            '%' => {
                if seen_wild || chars.peek().is_some() {
                    pure_prefix = false; // a `%` that isn't the sole trailing one
                }
                seen_wild = true;
                wildcard.push('*');
            }
            '_' => {
                seen_wild = true;
                pure_prefix = false;
                wildcard.push('?');
            }
            other => {
                if !seen_wild {
                    prefix.push(other);
                }
                push_escaped(&mut wildcard, other);
            }
        }
    }
    let mut body = serde_json::Map::new();
    if seen_wild && pure_prefix {
        body.insert("value".into(), json!(prefix));
        if case_insensitive {
            body.insert("case_insensitive".into(), json!(true));
        }
        json!({ "prefix": { col: Value::Object(body) } })
    } else {
        body.insert("value".into(), json!(wildcard));
        if case_insensitive {
            body.insert("case_insensitive".into(), json!(true));
        }
        json!({ "wildcard": { col: Value::Object(body) } })
    }
}

/// Escape a literal char for an OpenSearch wildcard value (its specials are `*`, `?`, `\`).
fn push_escaped(out: &mut String, c: char) {
    if matches!(c, '*' | '?' | '\\') {
        out.push('\\');
    }
    out.push(c);
}

/// A `col OP lit` or `lit OP col` binary as (column, literal-json), for the symmetric operators
/// (Eq/NotEq) where operand order doesn't change the clause.
fn col_lit(left: &Expr, right: &Expr) -> Option<(String, Value)> {
    if let (Some(c), Some(l)) = (column_name(left), literal_json(right)) {
        Some((c.to_string(), l))
    } else if let (Some(c), Some(l)) = (column_name(right), literal_json(left)) {
        Some((c.to_string(), l))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::{col, lit};
    use datafusion::prelude::Expr;

    #[test]
    fn eq_becomes_term_both_orderings() {
        let want = json!({ "term": { "status": "OK" } });
        assert_eq!(expr_to_query(&col("status").eq(lit("OK"))), Some(want.clone()));
        assert_eq!(expr_to_query(&lit("OK").eq(col("status"))), Some(want));
    }

    #[test]
    fn neq_becomes_must_not_term() {
        assert_eq!(
            expr_to_query(&col("status").not_eq(lit("DOWN"))),
            Some(json!({ "bool": { "must_not": { "term": { "status": "DOWN" } } } }))
        );
    }

    #[test]
    fn numeric_comparisons_become_range_and_flip_when_literal_first() {
        assert_eq!(
            expr_to_query(&col("speed").gt(lit(40_i64))),
            Some(json!({ "range": { "speed": { "gt": 40 } } }))
        );
        // 40 < speed  ≡  speed > 40
        assert_eq!(
            expr_to_query(&lit(40_i64).lt(col("speed"))),
            Some(json!({ "range": { "speed": { "gt": 40 } } }))
        );
        assert_eq!(
            expr_to_query(&col("speed").lt_eq(lit(60_i64))),
            Some(json!({ "range": { "speed": { "lte": 60 } } }))
        );
    }

    #[test]
    fn in_list_becomes_terms_and_negation_wraps_must_not() {
        assert_eq!(
            expr_to_query(&col("status").in_list(vec![lit("OK"), lit("DEGRADED")], false)),
            Some(json!({ "terms": { "status": ["OK", "DEGRADED"] } }))
        );
        assert_eq!(
            expr_to_query(&col("status").in_list(vec![lit("OK")], true)),
            Some(json!({ "bool": { "must_not": { "terms": { "status": ["OK"] } } } }))
        );
    }

    #[test]
    fn between_becomes_inclusive_range() {
        assert_eq!(
            expr_to_query(&col("speed").between(lit(40_i64), lit(60_i64))),
            Some(json!({ "range": { "speed": { "gte": 40, "lte": 60 } } }))
        );
    }

    #[test]
    fn is_not_null_becomes_exists() {
        assert_eq!(
            expr_to_query(&col("speed").is_not_null()),
            Some(json!({ "exists": { "field": "speed" } }))
        );
        assert_eq!(
            expr_to_query(&col("speed").is_null()),
            Some(json!({ "bool": { "must_not": { "exists": { "field": "speed" } } } }))
        );
    }

    #[test]
    fn boolean_composition_and_or_not() {
        let e = col("status").eq(lit("OK")).and(col("speed").gt(lit(10_i64)));
        assert_eq!(
            expr_to_query(&e),
            Some(
                json!({ "bool": { "filter": [ { "term": { "status": "OK" } }, { "range": { "speed": { "gt": 10 } } } ] } })
            )
        );
        let o = col("status").eq(lit("OK")).or(col("status").eq(lit("DEGRADED")));
        assert_eq!(
            expr_to_query(&o),
            Some(
                json!({ "bool": { "should": [ { "term": { "status": "OK" } }, { "term": { "status": "DEGRADED" } } ], "minimum_should_match": 1 } })
            )
        );
    }

    #[test]
    fn like_prefix_becomes_prefix_query_and_general_like_becomes_wildcard() {
        assert_eq!(
            expr_to_query(&col("reference").like(lit("SCALE-100%"))),
            Some(json!({ "prefix": { "reference": { "value": "SCALE-100" } } }))
        );
        assert_eq!(
            expr_to_query(&col("reference").like(lit("%SCALE_1%"))),
            Some(json!({ "wildcard": { "reference": { "value": "*SCALE?1*" } } }))
        );
        assert_eq!(
            expr_to_query(&col("reference").not_like(lit("X%"))),
            Some(json!({ "bool": { "must_not": { "prefix": { "reference": { "value": "X" } } } } }))
        );
        assert_eq!(
            expr_to_query(&col("reference").ilike(lit("scale%"))),
            Some(json!({ "prefix": { "reference": { "value": "scale", "case_insensitive": true } } }))
        );
    }

    #[cfg(feature = "udf")]
    #[test]
    fn match_udf_pushes_to_an_opensearch_match_query() {
        // The full-text `match` predicate, as an os_match UDF call, must push down to an analyzed
        // `match` query (inverted-index-backed) — not fall back to an in-memory scan.
        let e = crate::udf::os_match_udf().call(vec![col("reference"), lit("SCALE-5")]);
        assert_eq!(expr_to_query(&e), Some(json!({ "match": { "reference": "SCALE-5" } })));
        // A non-literal query is not pushable → None (DataFusion re-applies via the UDF).
        assert!(expr_to_query(&crate::udf::os_match_udf().call(vec![col("reference"), col("q")])).is_none());
    }

    #[cfg(feature = "udf")]
    #[test]
    fn geo_polygon_udf_pushes_to_a_native_geo_polygon_query() {
        let ring = "[[24.0,59.0],[26.0,59.0],[25.0,60.5]]";
        let e = crate::udf::os_geo_polygon_udf().call(vec![col("position"), lit(ring)]);
        assert_eq!(
            expr_to_query(&e),
            Some(json!({ "geo_polygon": { "position": { "points": [
                { "lat": 59.0, "lon": 24.0 }, { "lat": 59.0, "lon": 26.0 }, { "lat": 60.5, "lon": 25.0 },
            ] } } }))
        );
        // Under 3 points is not a polygon → not pushable (the UDF stub then fails closed).
        assert!(
            expr_to_query(&crate::udf::os_geo_polygon_udf().call(vec![col("position"), lit("[[1.0,2.0]]")])).is_none()
        );
    }

    #[test]
    fn temporal_literals_push_as_epoch_millis() {
        let ts = Expr::Literal(
            ScalarValue::TimestampMillisecond(Some(1_704_164_645_000), Some("UTC".into())),
            None,
        );
        assert_eq!(
            expr_to_query(&col("when").gt(ts)),
            Some(json!({ "range": { "when": { "gt": 1_704_164_645_000_i64 } } }))
        );
        let ns = Expr::Literal(
            ScalarValue::TimestampNanosecond(Some(1_704_164_645_000_000_000), None),
            None,
        );
        assert_eq!(
            expr_to_query(&col("when").eq(ns)),
            Some(json!({ "term": { "when": 1_704_164_645_000_i64 } }))
        );
        let day = Expr::Literal(ScalarValue::Date32(Some(19_724)), None); // 2024-01-02
        assert_eq!(
            expr_to_query(&col("when").gt_eq(day)),
            Some(json!({ "range": { "when": { "gte": 1_704_153_600_000_i64 } } }))
        );
    }

    #[test]
    fn untranslatable_filters_return_none_so_datafusion_reapplies() {
        assert!(expr_to_query(&col("a").eq(col("b"))).is_none()); // col = col, no literal
                                                                  // OR with one untranslatable side must NOT push down (would widen results).
        let bad_or = col("status").eq(lit("OK")).or(col("a").eq(col("b")));
        assert!(expr_to_query(&bad_or).is_none());
        // = NULL is not a term query.
        assert!(expr_to_query(&col("status").eq(Expr::Literal(ScalarValue::Utf8(None), None))).is_none());
    }
}
