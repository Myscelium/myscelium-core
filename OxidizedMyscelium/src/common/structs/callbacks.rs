// SPDX-License-Identifier: MPL-2.0
// Copyright © 2021-2026 Cristian Camargo Filho

use crate::common::functions::callbacks::call_callback;
use indexmap::IndexMap;
use parking_lot::Mutex;
use serde::Serialize;
use serde_json::Value;
use std::any::Any;
use std::boxed::Box;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

type Callback = dyn Fn(&[&dyn Any]) -> Box<dyn Any> + Send + Sync;
pub type CallbackClosure = Box<dyn Fn(Vec<Box<dyn Any + 'static>>) -> Box<dyn Any> + Send + Sync>;

// Vec<Box<(dyn Any + 'static)>>

#[derive(Clone)]
pub struct MyCallbacks {
    pub map: Arc<Mutex<HashMap<String, CallbackClosure>>>,
}

#[derive(Clone, Debug)]
pub enum CallbackError {
    CallbackDoNotExist(String),
    UnsupportedArgument(String),
    UnsupportedResponseArgument(String),
}

// fn convert_json_value_to_any(value: &Value) -> Option<Box<dyn Any>> {
//     match value {
//         Value::String(s) => Some(Box::new(s.clone()) as Box<dyn Any>),
//         Value::Number(n) => {
//             if n.is_i64() {
//                 n.as_i64().map(|i| Box::new(i) as Box<dyn Any>)
//             } else if n.is_f64() {
//                 n.as_f64().map(|f| Box::new(f) as Box<dyn Any>)
//             } else if n.is_u64() {
//                 n.as_u64().map(|u| Box::new(u) as Box<dyn Any>)
//             } else {
//                 None
//             }
//         },
//         Value::Bool(b) => Some(Box::new(*b) as Box<dyn Any>),
//         Value::Null => None,
//         Value::Array(arr) => {
//             let boxed_array: Vec<Box<dyn Any>> = arr.iter().filter_map(|item| convert_json_value_to_any(item)).collect();
//             Some(Box::new(boxed_array) as Box<dyn Any>)
//         },
//         Value::Object(obj) => {
//             let boxed_map: HashMap<String, Box<dyn Any>> = obj.iter().filter_map(|(k, v)| if let Some(boxed_value) = convert_json_value_to_any(v) { Some((k.clone(), boxed_value)) } else { None }).collect();
//             Some(Box::new(boxed_map) as Box<dyn Any>)
//         },
//     }
// }

impl MyCallbacks {
    pub fn new() -> Self {
        MyCallbacks {
            map: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn insert(&self, key: String, closure: CallbackClosure) {
        let mut map = self.map.lock();
        map.insert(key, closure);
    }

    /// Call the external callback
    ///
    /// This method requires
    /// - key -> is the key of the function that you wan to call
    /// - kwrags -> are the arguments to call thins function
    /// - args_pattern -> are the patterns to organize the arguments in the tuple to call it
    ///
    pub fn call(
        &self,
        key: &str,
        kwargs: HashMap<String, Value>,
        args_pattern: IndexMap<std::string::String, std::string::String>,
    ) -> Result<Box<dyn Any>, CallbackError> {
        let map = self.map.lock();
        if let Some(closure) = map.get(key) {
            //>----------------------------------------------------------------------------
            //> EXTRACT ARGS

            fn extract_strings_to_vec(map: IndexMap<String, String>) -> Vec<String> {
                let mut vec = Vec::new();

                // Iterate through the IndexMap and add both keys and values to the vector
                for (key, value) in map {
                    // TODO >>> Here is a nice place to put a type checking mechanism using the values that are the type required of the callback
                    vec.push(key); // Add key to the vector
                }

                vec
            }

            let args_pattern = extract_strings_to_vec(args_pattern);

            let mut args_pattern = args_pattern;
            let mut args: Vec<Box<dyn Any>> = Vec::new();

            //> Extract first arg that is a info carrier arg not contained inside the args_pattern
            let f_arg = kwargs.get("info").unwrap();
            let f_any: Box<dyn Any> = Box::new(f_arg.clone());

            args.push(f_any);

            println!("trying to reindex kwargs: {:?}", kwargs);

            //> Extract every other necessary argument and place in the correct order
            for arg_index in args_pattern {
                if let Some(arg) = kwargs.get(&arg_index) {
                    let any: Box<dyn Any> = Box::new(arg.clone());
                    args.push(any);
                } else {
                    println!("arg index: {:?} not found inside kwargs!", arg_index);
                }
            }

            // Convert Box<dyn Any> to &[&dyn Any] for callback
            let args_refs: Vec<&dyn Any> = args.iter().map(|arg| &**arg).collect();
            Ok(closure(args)) // Call the callback and return the response
        } else {
            Err(CallbackError::CallbackDoNotExist(key.to_string()))
        }
    }
}
