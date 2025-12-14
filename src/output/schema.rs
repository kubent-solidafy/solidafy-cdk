//! Arrow schema inference and JSON to Arrow conversion
//!
//! Provides utilities for inferring Arrow schemas from JSON data
//! and converting JSON records to Arrow RecordBatches.

use crate::error::{Error, Result};
use arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, ListArray, NullArray, StringArray,
    StructArray,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, Fields, Schema};
use arrow::record_batch::RecordBatch;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

/// Infer an Arrow schema from a set of JSON records
///
/// Analyzes all records to determine the most appropriate schema,
/// handling nullable fields and type variations.
pub fn infer_schema(records: &[Value]) -> Result<Schema> {
    if records.is_empty() {
        return Ok(Schema::empty());
    }

    let mut field_types: HashMap<String, DataType> = HashMap::new();

    for record in records {
        if let Value::Object(obj) = record {
            for (key, value) in obj {
                let inferred_type = infer_type(value);
                field_types
                    .entry(key.clone())
                    .and_modify(|existing| {
                        *existing = merge_types(existing, &inferred_type);
                    })
                    .or_insert(inferred_type);
            }
        }
    }

    let fields: Vec<Field> = field_types
        .into_iter()
        .map(|(name, dtype)| Field::new(name, dtype, true)) // All fields nullable
        .collect();

    Ok(Schema::new(fields))
}

/// Merge two schemas, combining fields from both
pub fn merge_schemas(schema1: &Schema, schema2: &Schema) -> Schema {
    let mut fields: HashMap<String, Field> = HashMap::new();

    for field in schema1.fields() {
        fields.insert(field.name().clone(), field.as_ref().clone());
    }

    for field in schema2.fields() {
        fields
            .entry(field.name().clone())
            .and_modify(|existing| {
                let merged_type = merge_types(existing.data_type(), field.data_type());
                *existing = Field::new(
                    existing.name(),
                    merged_type,
                    existing.is_nullable() || field.is_nullable(),
                );
            })
            .or_insert_with(|| field.as_ref().clone());
    }

    Schema::new(fields.into_values().collect::<Vec<_>>())
}

/// Convert JSON records to an Arrow RecordBatch
///
/// Uses the provided schema or infers one from the data.
pub fn json_to_arrow(records: &[Value], schema: Option<&Schema>) -> Result<RecordBatch> {
    let inferred = infer_schema(records)?;
    let schema = schema.unwrap_or(&inferred);

    if records.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::new(schema.clone())));
    }

    let mut columns: Vec<ArrayRef> = Vec::new();

    for field in schema.fields() {
        let values: Vec<Option<&Value>> = records
            .iter()
            .map(|record| {
                if let Value::Object(obj) = record {
                    obj.get(field.name())
                } else {
                    None
                }
            })
            .collect();

        let array = build_array(&values, field.data_type())?;
        columns.push(array);
    }

    RecordBatch::try_new(Arc::new(schema.clone()), columns).map_err(|e| Error::Output {
        message: format!("Failed to create RecordBatch: {e}"),
    })
}

/// Infer Arrow DataType from a JSON value
fn infer_type(value: &Value) -> DataType {
    match value {
        Value::Null => DataType::Null,
        Value::Bool(_) => DataType::Boolean,
        Value::Number(n) => {
            if n.is_i64() {
                DataType::Int64
            } else {
                DataType::Float64
            }
        }
        Value::String(_) => DataType::Utf8,
        Value::Array(arr) => {
            if arr.is_empty() {
                DataType::List(Arc::new(Field::new("item", DataType::Null, true)))
            } else {
                // Infer from first non-null element
                let element_type = arr
                    .iter()
                    .find(|v| !v.is_null())
                    .map_or(DataType::Null, infer_type);
                DataType::List(Arc::new(Field::new("item", element_type, true)))
            }
        }
        Value::Object(obj) => {
            let fields: Vec<Field> = obj
                .iter()
                .map(|(k, v)| Field::new(k, infer_type(v), true))
                .collect();
            DataType::Struct(Fields::from(fields))
        }
    }
}

/// Merge two data types into a compatible type
fn merge_types(type1: &DataType, type2: &DataType) -> DataType {
    match (type1, type2) {
        // Same types
        (a, b) if a == b => a.clone(),

        // Null can merge with anything
        (DataType::Null, other) | (other, DataType::Null) => other.clone(),

        // Numbers can merge (prefer Float64 for mixed)
        (DataType::Int64, DataType::Float64) | (DataType::Float64, DataType::Int64) => {
            DataType::Float64
        }

        // Different types -> fall back to String (most flexible)
        _ => DataType::Utf8,
    }
}

/// Build an Arrow array from JSON values
fn build_array(values: &[Option<&Value>], data_type: &DataType) -> Result<ArrayRef> {
    match data_type {
        DataType::Null => Ok(Arc::new(NullArray::new(values.len()))),

        DataType::Boolean => {
            let arr: BooleanArray = values.iter().map(|v| v.and_then(Value::as_bool)).collect();
            Ok(Arc::new(arr))
        }

        DataType::Int64 => {
            let arr: Int64Array = values.iter().map(|v| v.and_then(Value::as_i64)).collect();
            Ok(Arc::new(arr))
        }

        DataType::Float64 => {
            #[allow(clippy::cast_precision_loss)]
            let arr: Float64Array = values
                .iter()
                .map(|v| v.and_then(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64))))
                .collect();
            Ok(Arc::new(arr))
        }

        DataType::Utf8 => {
            let arr: StringArray = values
                .iter()
                .map(|v| {
                    v.map(|v| match v {
                        Value::String(s) => s.clone(),
                        _ => v.to_string(),
                    })
                })
                .collect();
            Ok(Arc::new(arr))
        }

        DataType::List(field) => build_list_array(values, field),

        DataType::Struct(fields) => build_struct_array(values, fields),

        _ => {
            // Fall back to string representation
            let arr: StringArray = values.iter().map(|v| v.map(ToString::to_string)).collect();
            Ok(Arc::new(arr))
        }
    }
}

/// Build a list array from JSON arrays
fn build_list_array(values: &[Option<&Value>], field: &Arc<Field>) -> Result<ArrayRef> {
    let mut all_items: Vec<Option<&Value>> = Vec::new();
    let mut offsets: Vec<i32> = vec![0];

    for value in values {
        if let Some(Value::Array(arr)) = value {
            for item in arr {
                all_items.push(Some(item));
            }
        }
        // Both array and non-array cases need an offset
        let offset = i32::try_from(all_items.len()).map_err(|_| Error::Output {
            message: "Array too large for i32 offset".to_string(),
        })?;
        offsets.push(offset);
    }

    let items_array = build_array(&all_items, field.data_type())?;
    let offset_buffer = OffsetBuffer::new(offsets.into());

    let list_array = ListArray::new(Arc::clone(field), offset_buffer, items_array, None);
    Ok(Arc::new(list_array))
}

/// Build a struct array from JSON objects
fn build_struct_array(values: &[Option<&Value>], fields: &Fields) -> Result<ArrayRef> {
    let mut child_arrays: Vec<ArrayRef> = Vec::new();

    for field in fields {
        let child_values: Vec<Option<&Value>> = values
            .iter()
            .map(|v| {
                v.and_then(|v| {
                    if let Value::Object(obj) = v {
                        obj.get(field.name())
                    } else {
                        None
                    }
                })
            })
            .collect();

        let child_array = build_array(&child_values, field.data_type())?;
        child_arrays.push(child_array);
    }

    let struct_array = StructArray::new(fields.clone(), child_arrays, None);
    Ok(Arc::new(struct_array))
}

/// Convert an Arrow RecordBatch to JSON records
///
/// Returns a vector of JSON objects, one per row in the batch.
pub fn arrow_to_json(batch: &RecordBatch) -> Result<Vec<Value>> {
    let schema = batch.schema();
    let num_rows = batch.num_rows();
    let mut records = Vec::with_capacity(num_rows);

    for row_idx in 0..num_rows {
        let mut record = serde_json::Map::new();

        for (col_idx, field) in schema.fields().iter().enumerate() {
            let column = batch.column(col_idx);
            let value = array_value_to_json(column.as_ref(), row_idx)?;
            record.insert(field.name().clone(), value);
        }

        records.push(Value::Object(record));
    }

    Ok(records)
}

/// Convert a single array element to JSON
#[allow(clippy::too_many_lines)]
fn array_value_to_json(array: &dyn arrow::array::Array, row: usize) -> Result<Value> {
    use arrow::array::Array;

    if array.is_null(row) {
        return Ok(Value::Null);
    }

    match array.data_type() {
        DataType::Null => Ok(Value::Null),

        DataType::Boolean => {
            let arr = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| Error::Output {
                    message: "Failed to downcast to BooleanArray".to_string(),
                })?;
            Ok(Value::Bool(arr.value(row)))
        }

        DataType::Int8 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Int8Array>()
                .ok_or_else(|| Error::Output {
                    message: "Failed to downcast to Int8Array".to_string(),
                })?;
            Ok(Value::Number(arr.value(row).into()))
        }

        DataType::Int16 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Int16Array>()
                .ok_or_else(|| Error::Output {
                    message: "Failed to downcast to Int16Array".to_string(),
                })?;
            Ok(Value::Number(arr.value(row).into()))
        }

        DataType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .ok_or_else(|| Error::Output {
                    message: "Failed to downcast to Int32Array".to_string(),
                })?;
            Ok(Value::Number(arr.value(row).into()))
        }

        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| Error::Output {
                    message: "Failed to downcast to Int64Array".to_string(),
                })?;
            Ok(Value::Number(arr.value(row).into()))
        }

        DataType::UInt8 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::UInt8Array>()
                .ok_or_else(|| Error::Output {
                    message: "Failed to downcast to UInt8Array".to_string(),
                })?;
            Ok(Value::Number(arr.value(row).into()))
        }

        DataType::UInt16 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::UInt16Array>()
                .ok_or_else(|| Error::Output {
                    message: "Failed to downcast to UInt16Array".to_string(),
                })?;
            Ok(Value::Number(arr.value(row).into()))
        }

        DataType::UInt32 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::UInt32Array>()
                .ok_or_else(|| Error::Output {
                    message: "Failed to downcast to UInt32Array".to_string(),
                })?;
            Ok(Value::Number(arr.value(row).into()))
        }

        DataType::UInt64 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
                .ok_or_else(|| Error::Output {
                    message: "Failed to downcast to UInt64Array".to_string(),
                })?;
            // UInt64 might overflow i64, so convert to string if needed
            let val = arr.value(row);
            if let Ok(signed) = i64::try_from(val) {
                Ok(Value::Number(signed.into()))
            } else {
                Ok(Value::String(val.to_string()))
            }
        }

        DataType::Float32 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Float32Array>()
                .ok_or_else(|| Error::Output {
                    message: "Failed to downcast to Float32Array".to_string(),
                })?;
            let val = f64::from(arr.value(row));
            Ok(serde_json::Number::from_f64(val).map_or(Value::Null, Value::Number))
        }

        DataType::Float64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| Error::Output {
                    message: "Failed to downcast to Float64Array".to_string(),
                })?;
            let val = arr.value(row);
            Ok(serde_json::Number::from_f64(val).map_or(Value::Null, Value::Number))
        }

        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| Error::Output {
                    message: "Failed to downcast to StringArray".to_string(),
                })?;
            Ok(Value::String(arr.value(row).to_string()))
        }

        DataType::LargeUtf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::LargeStringArray>()
                .ok_or_else(|| Error::Output {
                    message: "Failed to downcast to LargeStringArray".to_string(),
                })?;
            Ok(Value::String(arr.value(row).to_string()))
        }

        DataType::List(_) => {
            let arr = array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| Error::Output {
                    message: "Failed to downcast to ListArray".to_string(),
                })?;
            let values = arr.value(row);
            let mut items = Vec::with_capacity(values.len());
            for i in 0..values.len() {
                items.push(array_value_to_json(values.as_ref(), i)?);
            }
            Ok(Value::Array(items))
        }

        DataType::Struct(_) => {
            let arr = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| Error::Output {
                    message: "Failed to downcast to StructArray".to_string(),
                })?;
            let mut obj = serde_json::Map::new();
            for (i, field) in arr.fields().iter().enumerate() {
                let col = arr.column(i);
                let val = array_value_to_json(col.as_ref(), row)?;
                obj.insert(field.name().clone(), val);
            }
            Ok(Value::Object(obj))
        }

        // Default: convert to string representation
        _ => {
            // For other types, try to get a string representation
            Ok(Value::String(format!("{:?}", array.data_type())))
        }
    }
}
