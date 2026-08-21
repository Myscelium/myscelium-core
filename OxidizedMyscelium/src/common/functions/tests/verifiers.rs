// SPDX-License-Identifier: MPL-2.0
// Copyright © 2021-2026 Cristian Camargo Filho

use super::*;
use crate::common::functions::verifiers::{fast_json_comparator, ComparatorError};
use serde_json::json;

#[test]
fn test_empty_array_conversion() {
    let val = json!("[]");
    let target = json!([]);
    let result = fast_json_comparator(&val, &target);
    assert_eq!(result.unwrap(), json!([]));
}

#[test]
fn test_empty_object_conversion() {
    let val = json!("{}");
    let target = json!({});
    let result = fast_json_comparator(&val, &target);
    assert_eq!(result.unwrap(), json!({}));
}

#[test]
fn test_number_conversion() {
    let val = json!("42");
    let target = json!(42);
    let result = fast_json_comparator(&val, &target);
    assert_eq!(result.unwrap(), json!(42.0));
}

#[test]
fn test_boolean_conversion() {
    let val_true = json!("1");
    let target_true = json!(true);
    let result_true = fast_json_comparator(&val_true, &target_true);
    assert_eq!(result_true.unwrap(), json!(true));

    let val_false = json!("0");
    let target_false = json!(false);
    let result_false = fast_json_comparator(&val_false, &target_false);
    assert_eq!(result_false.unwrap(), json!(false));
}

#[test]
fn test_string_to_string() {
    let val = json!("hello");
    let target = json!("world");
    let result = fast_json_comparator(&val, &target);
    assert_eq!(result.unwrap(), json!("hello"));
}

#[test]
fn test_number_to_number() {
    let val = json!(42);
    let target = json!(42);
    let result = fast_json_comparator(&val, &target);
    assert_eq!(result.unwrap(), json!(42));
}

#[test]
fn test_bool_to_bool() {
    let val = json!(true);
    let target = json!(true);
    let result = fast_json_comparator(&val, &target);
    assert_eq!(result.unwrap(), json!(true));
}

#[test]
fn test_object_to_object() {
    let val = json!({"key1": "value1", "key2": "value2"});
    let target = json!({"key1": "value1"});
    let result = fast_json_comparator(&val, &target);
    let expected = json!({"key1": "value1"});
    assert_eq!(result.unwrap(), expected);
}

#[test]
fn test_array_to_array() {
    let val = json!(["value1", "value2"]);
    let target = json!(["value1", "value2"]);
    let result = fast_json_comparator(&val, &target);
    assert_eq!(result.unwrap(), json!(["value1", "value2"]));
}

#[test]
fn test_type_mismatch() {
    let val = json!(42);
    let target = json!("not a number");
    let result = fast_json_comparator(&val, &target);
    assert!(matches!(result, Err(ComparatorError::TypeMismatch(_))));
}

#[test]
fn test_missing_key() {
    let val = json!({"key1": "value1"});
    let target = json!({"key1": "value1", "key2": "value2"});
    let result = fast_json_comparator(&val, &target);
    assert!(matches!(result, Err(ComparatorError::MissingKey(_))));
}

#[test]
fn test_length_mismatch() {
    let val = json!(["value1"]);
    let target = json!(["value1", "value2"]);
    let result = fast_json_comparator(&val, &target);
    assert!(matches!(result, Err(ComparatorError::LengthMismatch)));
}

#[test]
fn test_target_is_empty() {
    let val = json!(["value1"]);
    let target = json!([]);
    let result = fast_json_comparator(&val, &target);
    assert!(matches!(result, Err(ComparatorError::TargetIsEmpty)));

    let val = json!({"key1": "value1"});
    let target = json!({});
    let result = fast_json_comparator(&val, &target);
    assert!(matches!(result, Err(ComparatorError::TargetIsEmpty)));
}
