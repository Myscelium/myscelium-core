// SPDX-License-Identifier: MPL-2.0
// Copyright © 2021-2026 Cristian Camargo Filho

use crate::common::enhanced_buffer::utilities::Command;
use crate::common::functions::converters::*;
use crate::common::structs::results_structs::ResultType;
use serde_json::{json, Map, Value};
use std::collections::HashMap;
use std::sync::MutexGuard;

#[test]
fn test_convert_to_resulttype_map() {
    let mut input_map = HashMap::new();
    input_map.insert("key1".to_string(), ResultType::Str("value1".to_string()));

    let mut nested_map = HashMap::new();
    nested_map.insert(
        "nested_key".to_string(),
        ResultType::Str("nested_value".to_string()),
    );
    input_map.insert("key2".to_string(), ResultType::Map(nested_map.clone()));

    let result = convert_to_resulttype_map(&input_map);

    let mut expected_map = HashMap::new();
    expected_map.insert("key1".to_string(), ResultType::Str("value1".to_string()));
    expected_map.insert("key2".to_string(), ResultType::Map(nested_map));

    assert_eq!(result, expected_map);
}

#[test]
fn test_value_to_resulttype() {
    assert_eq!(
        value_to_resulttype(&json!("string")),
        Ok(ResultType::Str("string".to_string()))
    );
    assert_eq!(value_to_resulttype(&json!(42)), Ok(ResultType::Int(42)));
    assert_eq!(
        value_to_resulttype(&json!(3.14)),
        Ok(ResultType::Float(3.14))
    );
    assert_eq!(
        value_to_resulttype(&json!(true)),
        Ok(ResultType::Bool(true))
    );
    assert_eq!(value_to_resulttype(&json!(null)), Ok(ResultType::Empty));

    let mut map = HashMap::new();
    map.insert("key".to_string(), ResultType::Str("value".to_string()));
    assert_eq!(
        value_to_resulttype(&json!({"key": "value"})),
        Ok(ResultType::Map(map.clone()))
    );

    let list = vec![ResultType::Str("value".to_string())];
    assert_eq!(
        value_to_resulttype(&json!(["value"])),
        Ok(ResultType::List(list))
    );

    assert!(matches!(
        value_to_resulttype(&json!(42.0)),
        Ok(ResultType::Float(42.0))
    ));
    assert!(matches!(
        value_to_resulttype(&json!(42.0)),
        Ok(ResultType::Float(42.0))
    ));
    assert!(matches!(
        value_to_resulttype(&json!({"key": "value"})),
        Ok(ResultType::Map(_))
    ));
    assert!(matches!(
        value_to_resulttype(&json!(["value"])),
        Ok(ResultType::List(_))
    ));
}

#[test]
fn test_resulttype_to_value() {
    assert_eq!(
        resulttype_to_value(&ResultType::Str("string".to_string())),
        json!("string")
    );
    assert_eq!(resulttype_to_value(&ResultType::Int(42)), json!(42));
    assert_eq!(resulttype_to_value(&ResultType::Float(3.14)), json!(3.14));
    assert_eq!(resulttype_to_value(&ResultType::Bool(true)), json!(true));
    assert_eq!(resulttype_to_value(&ResultType::Empty), json!(null));

    let mut map = HashMap::new();
    map.insert("key".to_string(), ResultType::Str("value".to_string()));
    assert_eq!(
        resulttype_to_value(&ResultType::Map(map.clone())),
        json!({"key": "value"})
    );

    let list = vec![ResultType::Str("value".to_string())];
    assert_eq!(
        resulttype_to_value(&ResultType::List(list.clone())),
        json!(["value"])
    );
}

#[test]
fn test_convert_to_value_map() {
    let mut input_map = HashMap::new();
    input_map.insert("key1".to_string(), ResultType::Str("value1".to_string()));
    input_map.insert("key2".to_string(), ResultType::Int(42));

    let result = convert_to_value_map(&input_map);

    let mut expected_map = HashMap::new();
    expected_map.insert("key1".to_string(), json!("value1"));
    expected_map.insert("key2".to_string(), json!(42));

    assert_eq!(result, expected_map);
}

#[test]
fn test_convert_value_map_to_resulttype_map() {
    let mut input_map = HashMap::new();
    input_map.insert("key1".to_string(), json!("value1"));
    input_map.insert("key2".to_string(), json!(42));

    let result = convert_value_map_to_resulttype_map(&input_map).unwrap();

    let mut expected_map = HashMap::new();
    expected_map.insert("key1".to_string(), ResultType::Str("value1".to_string()));
    expected_map.insert("key2".to_string(), ResultType::Int(42));

    assert_eq!(result, ResultType::Map(expected_map));
}

#[test]
fn test_convert_json_map_to_hash_map() {
    let json_map: Map<String, Value> =
        serde_json::from_str(r#"{"key1": "value1", "key2": 42}"#).unwrap();
    let result = convert_json_map_to_hash_map(&json_map);

    let mut expected_map = HashMap::new();
    expected_map.insert("key1".to_string(), json!("value1"));
    expected_map.insert("key2".to_string(), json!(42));

    assert_eq!(result, expected_map);
}
