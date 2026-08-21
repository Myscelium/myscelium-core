// SPDX-License-Identifier: MPL-2.0
// Copyright © 2021-2026 Cristian Camargo Filho

use crate::common::enhanced_buffer::utilities::Command;
use crate::common::enhanced_buffer::utilities::CommandInstructions;
use crate::common::enhanced_buffer::utilities::CommandType;
use crate::common::structs::results_structs::ResultType;

use parking_lot::Mutex;
use parking_lot::MutexGuard;
use serde_json::Value as JsonValue;
use serde_json::{json, Value};

use std::any::Any;
use std::collections::HashMap;
use std::result;
use std::sync::Arc;

trait JsonToAny {
    fn as_any(&self) -> &dyn Any;
}

impl JsonToAny for i64 {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl JsonToAny for String {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

fn convert_json_value_to_any(value: &Value) -> Option<Box<dyn Any>> {
    match value {
        Value::String(s) => Some(Box::new(s.clone()) as Box<dyn Any>),
        Value::Number(n) if n.is_i64() => n.as_i64().map(|i| Box::new(i) as Box<dyn Any>),
        // Handle other types as needed
        _ => None,
    }
}

pub fn call_callback(
    key: &str,
    kwargs: HashMap<String, Value>,
    callback: &Box<dyn Fn(&[&dyn Any]) -> Box<dyn Any> + Send + Sync>,
) -> Result<Box<dyn Any>, String> {
    let mut args: Vec<Box<dyn Any>> = Vec::new();
    for (_key, value) in kwargs {
        if let Some(any) = convert_json_value_to_any(&value) {
            args.push(any);
        } else {
            return Err(format!("Unsupported argument type for key: {}", _key));
        }
    }

    // Convert Box<dyn Any> to &[&dyn Any] for callback
    let args_refs: Vec<&dyn Any> = args.iter().map(|arg| &**arg).collect();

    Ok(callback(&args_refs))
}
