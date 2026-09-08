//! Arrow schema handling: deriving a schema from an index `_mapping`, and turning `_source`
//! documents into a `RecordBatch` for a (possibly projected) schema.

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
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
/// | `keyword`, `text`, `wildcard`, `constant_keyword`, `ip`, `date`, `date_nanos` | `Utf8` |
/// | `long`, `integer`, `short`, `byte`, `unsigned_long` | `Int64` |
/// | `double`, `float`, `half_float`, `scaled_float` | `Float64` |
/// | `boolean` | `Boolean` |
/// | `object`, `nested`, `geo_point`, `geo_shape`, anything else | `Utf8` (the value's JSON text) |
///
/// Only `Utf8` / `Int64` / `Float64` / `Boolean` columns are materialised (see [`build_batch`]), so
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
        // text-like, temporal (ISO strings), structured, geo, and unknown → text.
        _ => DataType::Utf8,
    }
}

/// Build a `RecordBatch` for `schema` from `_source` objects, reading each field by name and
/// coercing to the declared Arrow type. Absent, null, or type-mismatched values become nulls.
///
/// Coercions: a `Utf8` column reads a string as-is and any other non-null JSON value (object,
/// array, number, bool) as its compact JSON text; an `Int64` column also accepts a whole-valued
/// float (`106.0`), which OpenSearch/JSON commonly store for integer fields.
pub fn build_batch(schema: &SchemaRef, sources: &[Value]) -> Result<RecordBatch> {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let name = field.name();
        let col: ArrayRef = match field.data_type() {
            DataType::Utf8 => {
                let mut b = StringBuilder::new();
                for s in sources {
                    b.append_option(match s.get(name) {
                        None | Some(Value::Null) => None,
                        Some(Value::String(s)) => Some(s.clone()),
                        Some(other) => Some(other.to_string()),
                    });
                }
                Arc::new(b.finish())
            }
            DataType::Int64 => {
                let mut b = Int64Builder::new();
                for s in sources {
                    b.append_option(s.get(name).and_then(|v| {
                        v.as_i64().or_else(|| v.as_f64().filter(|f| f.fract() == 0.0 && f.is_finite()).map(|f| f as i64))
                    }));
                }
                Arc::new(b.finish())
            }
            DataType::Float64 => {
                let mut b = Float64Builder::new();
                for s in sources {
                    b.append_option(s.get(name).and_then(Value::as_f64));
                }
                Arc::new(b.finish())
            }
            DataType::Boolean => {
                let mut b = BooleanBuilder::new();
                for s in sources {
                    b.append_option(s.get(name).and_then(Value::as_bool));
                }
                Arc::new(b.finish())
            }
            other => {
                return Err(DataFusionError::NotImplemented(format!(
                    "datafusion-opensearch: column '{name}' has unsupported type {other:?} (Utf8/Int64/Float64/Boolean only)"
                )))
            }
        };
        columns.push(col);
    }
    RecordBatch::try_new(schema.clone(), columns).map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringArray};
    use datafusion::arrow::datatypes::TimeUnit;
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
                ("when".into(), DataType::Utf8),  // date → ISO text
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
    fn build_batch_rejects_unsupported_column_type() {
        let s = Arc::new(Schema::new(vec![Field::new(
            "t",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        )]));
        let err = build_batch(&s, &[json!({ "t": 1 })]).unwrap_err();
        assert!(matches!(err, DataFusionError::NotImplemented(_)));
    }
}
