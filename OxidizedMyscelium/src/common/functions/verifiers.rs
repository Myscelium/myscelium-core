// SPDX-License-Identifier: MPL-2.0
// Copyright © 2021-2026 Cristian Camargo Filho

use serde_json::{Error as JsonError, Value};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub enum ComparatorError {
    MissingKey(String),
    LengthMismatch,
    TargetIsEmpty,
    TypeMismatch(Value),
    ParseError(String),
}

/// Compares two JSON values, `val` and `target`, and attempts to convert `val` to match the structure
/// and types of `target`. This function handles basic conversions like integer to boolean or
/// string to integer, and ensures structural similarity for objects and arrays.
///
/// # Arguments
///
/// * `val` - The JSON value to be compared and converted.
/// * `target` - The target JSON structure to compare against.
///
/// # Returns
///
/// A `Result` containing either the converted `Value` or a `ComparatorError`.
///
/// # Examples
///
/// ```
/// let val = serde_json::json!({"key1": "42", "key2": ["1", "0"]});
/// let target = serde_json::json!({"key1": 42, "key2": [true, false]});
/// let result = fast_json_comparator(&val, &target);
/// assert!(result.is_ok());
/// ```
pub fn fast_json_comparator(val: &Value, target: &Value) -> Result<Value, ComparatorError> {
    println!(
        "\n\nCompraing json val:\n{}\nwith json pattern val:\n{}\n\n",
        &val, &target
    );

    match (val, target) {
        // Convert string "[]" to empty array
        (Value::String(s), Value::Array(pattern_arr)) if s == "[]" && pattern_arr.is_empty() => {
            Ok(Value::Array(vec![]))
        }

        // Convert string "{}" to empty object
        (Value::String(s), Value::Object(pattern_obj)) if s == "{}" && pattern_obj.is_empty() => {
            Ok(Value::Object(serde_json::Map::new()))
        }

        (Value::Object(obj), Value::Object(pattern_obj)) => {
            if pattern_obj.is_empty() {
                return Err(ComparatorError::TargetIsEmpty);
            }

            let mut new_obj: HashMap<String, Value> = HashMap::new();
            for (k, pv) in pattern_obj {
                match obj.get(k) {
                    Some(v) => new_obj.insert(k.clone(), fast_json_comparator(v, pv)?),
                    None => return Err(ComparatorError::MissingKey(k.clone())),
                };
            }
            Ok(Value::Object(serde_json::Map::from_iter(
                new_obj.into_iter(),
            )))
        }

        (Value::Array(arr), Value::Array(pattern_arr)) => {
            if pattern_arr.is_empty() {
                return Err(ComparatorError::TargetIsEmpty);
            }

            if arr.len() != pattern_arr.len() {
                return Err(ComparatorError::LengthMismatch);
            }

            let new_arr: Result<Vec<_>, _> = arr
                .iter()
                .zip(pattern_arr.iter())
                .map(|(elem, pattern_elem)| fast_json_comparator(elem, pattern_elem))
                .collect();

            Ok(Value::Array(new_arr?))
        }

        // Convert string to number
        (Value::String(s), Value::Number(_)) => {
            if let Ok(num) = s.parse::<f64>() {
                Ok(Value::Number(serde_json::Number::from_f64(num).unwrap()))
            } else {
                Err(ComparatorError::TypeMismatch(val.clone()))
            }
        }

        // Convert string "1" or "0" to boolean
        (Value::String(s), Value::Bool(_)) => match s.as_str() {
            "1" => Ok(Value::Bool(true)),
            "0" => Ok(Value::Bool(false)),
            _ => Err(ComparatorError::TypeMismatch(val.clone())),
        },

        // Compare types without checking content
        (Value::String(_), Value::String(_)) => Ok(val.clone()),
        (Value::Number(_), Value::Number(_)) => Ok(val.clone()),
        (Value::Bool(_), Value::Bool(_)) => Ok(val.clone()),

        // For other types, check if they are the same
        _ => {
            if val == target {
                Ok(val.clone())
            } else {
                Err(ComparatorError::TypeMismatch(val.clone()))
            }
        }
    }
}
