//! Arrow schema handling: deriving a schema from an index `_mapping`, and turning `_source`
//! documents into a `RecordBatch` for a (possibly projected) schema.

use std::sync::Arc;

use std::str::FromStr;

use datafusion::arrow::array::timezone::Tz;
use datafusion::arrow::array::{
    ArrayRef, BooleanArray, Float32Array, Float64Array, LargeStringArray, PrimitiveArray, StringArray, StringViewArray,
    TimestampMillisecondArray,
};
use datafusion::arrow::compute::kernels::cast_utils::string_to_datetime;
use datafusion::arrow::datatypes::{
    ArrowPrimitiveType, DataType, Field, Int16Type, Int32Type, Int64Type, Int8Type, Schema, SchemaRef, TimeUnit,
    UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use serde_json::Value;

/// Derive an Arrow schema from an OpenSearch `_mapping`.
///
/// Accepts either the raw `GET /<index>/_mapping` response (`{ "<index>": { "mappings": { … } } }`,
/// the first index is used), a bare `mappings` object, or a bare `properties` object. Fields are
/// emitted sorted by name so the schema is deterministic.
///
/// Mapping type → Arrow type:
///
/// | OpenSearch | Arrow |
/// |---|---|
/// | `keyword`, `text`, `wildcard`, `constant_keyword`, `ip` | `Utf8` |
/// | `date`, `date_nanos` | `Timestamp(Millisecond, UTC)` |
/// | `long`, `integer`, `short`, `byte`, `unsigned_long` | `Int64` |
/// | `double`, `float`, `half_float`, `scaled_float` | `Float64` |
/// | `boolean` | `Boolean` |
/// | `object`, `nested`, `geo_point`, `geo_shape`, anything else | `Utf8` (the value's JSON text) |
///
/// Only `Utf8` / `Int64` / `Float64` / `Boolean` / `Timestamp(Millisecond)` columns are materialised (see [`build_batch`]), so
/// every mapping type is readable — structured and unknown types arrive as JSON text to parse
/// downstream. Every field is nullable: documents routinely omit fields. Returns `None` when no
/// `properties` can be found.
pub fn schema_from_mapping(mapping: &Value) -> Option<SchemaRef> {
    let properties = find_properties(mapping)?;
    let mut fields: Vec<Field> = properties
        .iter()
        .map(|(name, def)| Field::new(name, arrow_type_for(def.get("type").and_then(Value::as_str)), true))
        .collect();
    fields.sort_by(|a, b| a.name().cmp(b.name()));
    Some(Arc::new(Schema::new(fields)))
}

/// Locate the `properties` object in any of the accepted shapes.
fn find_properties(v: &Value) -> Option<&serde_json::Map<String, Value>> {
    if let Some(p) = v.get("properties").and_then(Value::as_object) {
        return Some(p);
    }
    if let Some(m) = v.get("mappings") {
        return m.get("properties").and_then(Value::as_object);
    }
    // Raw response: keyed by index name; take the first index.
    v.as_object()?.values().next().and_then(find_properties)
}

/// Map one OpenSearch field type name to the Arrow type this crate reads it as.
fn arrow_type_for(os_type: Option<&str>) -> DataType {
    match os_type {
        Some("long" | "integer" | "short" | "byte" | "unsigned_long") => DataType::Int64,
        Some("double" | "float" | "half_float" | "scaled_float") => DataType::Float64,
        Some("boolean") => DataType::Boolean,
        Some("date" | "date_nanos") => DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
        // text-like, structured, geo, and unknown → text.
        _ => DataType::Utf8,
    }
}

/// Build a `RecordBatch` for `schema` from `_source` objects, reading each field by name and
/// coercing to the declared Arrow type. Absent, null, or type-mismatched values become nulls.
///
/// Supported column types: `Utf8` / `LargeUtf8` / `Utf8View` (a string as-is, any other non-null
/// JSON value as its compact JSON text), every integer width `Int8`…`Int64` / `UInt8`…`UInt64`
/// (a whole-valued float such as `106.0` is accepted; out-of-range values become null), `Float32`
/// / `Float64`, `Boolean`, and `Timestamp(Millisecond, _)` (an ISO 8601 / RFC 3339 string or an
/// epoch-milliseconds number — the two shapes OpenSearch's default `date` format accepts).
pub fn build_batch(schema: &SchemaRef, sources: &[Value]) -> Result<RecordBatch> {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let name = field.name();
        let values = sources.iter().map(|s| s.get(name));
        let col: ArrayRef = match field.data_type() {
            DataType::Utf8 => Arc::new(values.map(json_text).collect::<StringArray>()),
            DataType::LargeUtf8 => Arc::new(values.map(json_text).collect::<LargeStringArray>()),
            DataType::Utf8View => Arc::new(values.map(json_text).collect::<StringViewArray>()),
            DataType::Int8 => int_column::<Int8Type>(values),
            DataType::Int16 => int_column::<Int16Type>(values),
            DataType::Int32 => int_column::<Int32Type>(values),
            DataType::Int64 => int_column::<Int64Type>(values),
            DataType::UInt8 => int_column::<UInt8Type>(values),
            DataType::UInt16 => int_column::<UInt16Type>(values),
            DataType::UInt32 => int_column::<UInt32Type>(values),
            DataType::UInt64 => int_column::<UInt64Type>(values),
            DataType::Float32 => Arc::new(
                values
                    .map(|v| v.and_then(Value::as_f64).map(|f| f as f32))
                    .collect::<Float32Array>(),
            ),
            DataType::Float64 => Arc::new(values.map(|v| v.and_then(Value::as_f64)).collect::<Float64Array>()),
            DataType::Boolean => Arc::new(values.map(|v| v.and_then(Value::as_bool)).collect::<BooleanArray>()),
            DataType::Timestamp(TimeUnit::Millisecond, tz) => {
                let utc = Tz::from_str("UTC").expect("UTC is a valid timezone");
                let millis = values.map(|v| match v {
                    Some(Value::Number(n)) => n.as_i64().or_else(|| n.as_f64().map(|f| f as i64)),
                    Some(Value::String(text)) => string_to_datetime(&utc, text).ok().map(|dt| dt.timestamp_millis()),
                    _ => None,
                });
                Arc::new(
                    millis
                        .collect::<TimestampMillisecondArray>()
                        .with_timezone_opt(tz.clone()),
                )
            }
            other => {
                return Err(DataFusionError::NotImplemented(format!(
                    "datafusion-opensearch: column '{name}' has unsupported type {other:?} \
                     (string, integer, float, Boolean and Timestamp(Millisecond) columns only)"
                )))
            }
        };
        columns.push(col);
    }
    RecordBatch::try_new(schema.clone(), columns).map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

/// A JSON value as text: strings as-is, anything else non-null as compact JSON.
fn json_text(v: Option<&Value>) -> Option<String> {
    match v {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) => Some(s.clone()),
        Some(other) => Some(other.to_string()),
    }
}

/// A JSON number as an integer: integers as-is, whole-valued finite floats converted, anything
/// out of the target range → null.
fn int_column<'a, T>(values: impl Iterator<Item = Option<&'a Value>>) -> ArrayRef
where
    T: ArrowPrimitiveType,
    T::Native: TryFrom<i64>,
{
    Arc::new(
        values
            .map(|v| {
                let n = v.and_then(|v| {
                    v.as_i64().or_else(|| {
                        v.as_f64()
                            .filter(|f| f.fract() == 0.0 && f.is_finite())
                            .map(|f| f as i64)
                    })
                })?;
                T::Native::try_from(n).ok()
            })
            .collect::<PrimitiveArray<T>>(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringArray};
    use serde_json::json;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("status", DataType::Utf8, true),
            Field::new("speed", DataType::Float64, true),
            Field::new("active", DataType::Boolean, true),
        ]))
    }

    #[test]
    fn mapping_types_map_to_arrow_and_fields_sort_by_name() {
        let raw = json!({ "my-index": { "mappings": { "properties": {
            "name": { "type": "keyword" },
            "body": { "type": "text" },
            "count": { "type": "long" },
            "ratio": { "type": "double" },
            "flag": { "type": "boolean" },
            "when": { "type": "date" },
            "where": { "type": "geo_point" },
            "meta": { "properties": { "k": { "type": "keyword" } } },
            "odd": { "type": "some_future_type" },
        } } } });
        let s = schema_from_mapping(&raw).unwrap();
        let types: Vec<(String, DataType)> = s
            .fields()
            .iter()
            .map(|f| (f.name().clone(), f.data_type().clone()))
            .collect();
        assert_eq!(
            types,
            vec![
                ("body".into(), DataType::Utf8),
                ("count".into(), DataType::Int64),
                ("flag".into(), DataType::Boolean),
                ("meta".into(), DataType::Utf8), // object → JSON text
                ("name".into(), DataType::Utf8),
                ("odd".into(), DataType::Utf8), // unknown → text
                ("ratio".into(), DataType::Float64),
                (
                    "when".into(),
                    DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
                ),
                ("where".into(), DataType::Utf8), // geo_point → JSON text
            ]
        );
        assert!(s.fields().iter().all(|f| f.is_nullable()));
    }

    #[test]
    fn mapping_accepts_bare_mappings_and_bare_properties() {
        let props = json!({ "a": { "type": "integer" } });
        assert_eq!(
            schema_from_mapping(&json!({ "mappings": { "properties": props } }))
                .unwrap()
                .fields()
                .len(),
            1
        );
        assert_eq!(
            schema_from_mapping(&json!({ "properties": props }))
                .unwrap()
                .fields()
                .len(),
            1
        );
        assert!(schema_from_mapping(&json!({ "nothing": "here" })).is_none());
    }

    #[test]
    fn build_batch_coerces_by_declared_type_and_nulls_absent() {
        let s = schema();
        let sources = vec![
            json!({ "id": "r1", "status": "OK", "speed": 42.5, "active": true }),
            json!({ "id": "r2", "speed": 3 }), // status absent → null; speed int coerces to f64
        ];
        let batch = build_batch(&s, &sources).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 4);
        let status = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(status.value(0), "OK");
        assert!(status.is_null(1));
        let speed = batch.column(2).as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(speed.value(1), 3.0);
        // An Int64 column whose source value is a whole float (106.0) coerces; a fraction nulls.
        let s2 = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)]));
        let b2 = build_batch(&s2, &[json!({ "n": 106.0 }), json!({ "n": 7 }), json!({ "n": 1.5 })]).unwrap();
        let n = b2.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(n.value(0), 106);
        assert_eq!(n.value(1), 7);
        assert!(n.is_null(2));
        let active = batch.column(3).as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(active.value(0));
        assert!(active.is_null(1));
    }

    #[test]
    fn build_batch_numeric_mismatch_nulls_but_utf8_stringifies() {
        let sources = vec![json!({ "id": 12345, "status": {"nested": 1}, "speed": "fast", "active": "yes" })];
        let batch = build_batch(&schema(), &sources).unwrap();
        assert_eq!(
            batch.column(0).as_any().downcast_ref::<StringArray>().unwrap().value(0),
            "12345"
        );
        assert_eq!(
            batch.column(1).as_any().downcast_ref::<StringArray>().unwrap().value(0),
            "{\"nested\":1}"
        );
        assert!(batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .is_null(0));
        assert!(batch
            .column(3)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .is_null(0));
    }

    #[test]
    fn build_batch_empty_sources_is_zero_row_batch() {
        let batch = build_batch(&schema(), &[]).unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 4);
    }

    #[test]
    fn build_batch_reads_timestamps_from_iso_strings_and_epoch_millis() {
        use datafusion::arrow::array::TimestampMillisecondArray;
        let s = Arc::new(Schema::new(vec![Field::new(
            "t",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            true,
        )]));
        let b = build_batch(
            &s,
            &[
                json!({ "t": "2024-01-02T03:04:05Z" }),
                json!({ "t": "2024-01-02T03:04:05.678+02:00" }),
                json!({ "t": 1_704_164_645_000_i64 }),
                json!({ "t": "2024-01-02" }),
                json!({ "t": "not a date" }),
                json!({}),
            ],
        )
        .unwrap();
        let t = b
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        assert_eq!(t.value(0), 1_704_164_645_000);
        assert_eq!(t.value(1), 1_704_164_645_678 - 2 * 3_600_000);
        assert_eq!(t.value(2), 1_704_164_645_000);
        assert_eq!(t.value(3), 1_704_153_600_000);
        assert!(t.is_null(4) && t.is_null(5));
    }

    #[test]
    fn build_batch_materialises_every_declared_width_and_string_flavour() {
        use datafusion::arrow::array::{Int32Array, StringViewArray, UInt8Array};
        let s = Arc::new(Schema::new(vec![
            Field::new("v", DataType::Utf8View, true),
            Field::new("i32", DataType::Int32, true),
            Field::new("u8", DataType::UInt8, true),
            Field::new("f32", DataType::Float32, true),
        ]));
        let b = build_batch(
            &s,
            &[
                json!({ "v": "x", "i32": 7, "u8": 255, "f32": 1.5 }),
                json!({ "v": { "k": 1 }, "i32": 3_000_000_000_i64, "u8": -1, "f32": 2 }),
            ],
        )
        .unwrap();
        assert_eq!(
            b.column(0).as_any().downcast_ref::<StringViewArray>().unwrap().value(1),
            "{\"k\":1}"
        );
        let i = b.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(i.value(0), 7);
        assert!(i.is_null(1), "out of range → null");
        let u = b.column(2).as_any().downcast_ref::<UInt8Array>().unwrap();
        assert_eq!(u.value(0), 255);
        assert!(u.is_null(1), "negative → null");
        assert_eq!(
            b.column(3).as_any().downcast_ref::<Float32Array>().unwrap().value(1),
            2.0
        );
    }

    #[test]
    fn build_batch_rejects_unsupported_column_type() {
        let s = Arc::new(Schema::new(vec![Field::new("t", DataType::Date32, true)]));
        let err = build_batch(&s, &[json!({ "t": 1 })]).unwrap_err();
        assert!(matches!(err, DataFusionError::NotImplemented(_)));
    }
}
