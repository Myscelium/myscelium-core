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

fn call_callback(
    key: &str,
    kwargs: HashMap<String, Value>,
    callback_patterns: MutexGuard<
        HashMap<&'static str, Box<dyn Fn(&[&dyn Any]) -> Box<dyn Any> + Send + Sync>>,
    >,
) -> Result<Box<dyn Any>, String> {
    if let Some(callback) = callback_patterns.get(key) {
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
    } else {
        Err(format!("Callback with key '{}' not found!", key))
    }
}

// pub fn call_callback(
//     key: &str,
//     args: &[&dyn Any],
//     callback_patterns: MutexGuard<
//         HashMap<&'static str, Box<dyn Fn(&[&dyn Any]) -> Box<dyn Any> + Send + Sync>>,
//     >,
// ) -> Option<Box<dyn Any>> {
//     callback_patterns.get(key).map(|callback| callback(args))
// }

// pub fn call_callback(command: Command) -> Option<HashMap<String, Value>> {
//     if let Some(callback) = callback_patterns.get(&command.command.actf) {
//         callback(command.kwargs); // Call the callback function if found
//     } else {
//         println!("Callback with key '{}' not found!", &command.command.actf);
//     }

//     let function_name: &String = &command.command.actf;

//     // Get the function and args_types from the CALLBACK_PATTERNS
//     let (function, _) = callback_patterns.get(function_name).unwrap();

//     let command: &CommandInstructions = &command.command;

//     let inner_hash_map: HashMap<_, _> = command.kwargs.clone().into_iter().collect();
//     let kwargs_map: HashMap<String, Py<PyAny>> =
//         dict_to_kwargs(py, &inner_hash_map).map_err(|e| {
//             PyErr::new::<PyException, _>(format!(
//                 "Error converting arguments to kwargs to call client callback: {:?}",
//                 e
//             ))
//         })?;

//     // let kwargs_map = match command.command_type() {
//     //     CommandType::Response(_) => {
//     //         let command = &command.command;

//     //         let inner_hash_map: HashMap<_, _> = command.kwargs.clone().into_iter().collect();
//     //         dict_to_kwargs(py, &inner_hash_map).map_err(|e| PyErr::new::<PyException, _>(format!("Error converting arguments to kwargs to call client callback: {:?}", e)))?
//     //     },
//     //     CommandType::Function(_) => {
//     //         let command = &command.command;

//     //         let inner_hash_map: HashMap<_, _> = command.kwargs.clone().into_iter().collect();
//     //         dict_to_kwargs(py, &inner_hash_map).map_err(|e| PyErr::new::<PyException, _>(format!("Error converting arguments to kwargs to call client callback: {:?}", e)))?
//     //     },

//     //     _ => dict_to_kwargs(py, &command.command).map_err(|e| PyErr::new::<PyException, _>(format!("Error converting arguments to kwargs to call client callback: {:?}", e)))?,
//     // };

//     println!("Converted to Python kwargs_map: {:?}", kwargs_map);

//     // -> Convert to py dict
//     let kwargs = PyDict::new(py);
//     for (key, value) in kwargs_map {
//         kwargs.set_item(key, value).unwrap();
//     }

//     // Call the Python function with the converted arguments
//     let result = function.call(py, (), Some(kwargs)).map_err(|e| e)?;

//     let result_obj: PyObject = result.clone().into(); // Convert the result into a PyObject

//     Ok(result_obj) // Return the PyObject
// }

// pub fn client_call_callback(
//     command: &Command,
//     callback_patterns: std::collections::HashMap<
//         &'static str,
//         Box<dyn Fn() + Send + Sync + 'static>,
//     >,
// ) -> PyResult<PyObject> {
//     println!("Command to call a callback: {:?}", command);

//     let function_name: &String = &command.command.actf;

//     // Get the function and args_types from the CALLBACK_PATTERNS
//     let (function, _) = callback_patterns.get(function_name).unwrap();

//     let command: &CommandInstructions = &command.command;

//     let inner_hash_map: HashMap<String, Value> = command.convert_to_hashmap_string_value();

//     let mut kwargs_dict: HashMap<String, Value> = HashMap::new();

//     kwargs_dict.insert(
//         "data".to_string(),
//         Value::Object(serde_json::Map::from_iter(inner_hash_map)),
//     );

//     let kwargs_map: HashMap<String, Py<PyAny>> = dict_to_kwargs(py, &kwargs_dict).map_err(|e| {
//         PyErr::new::<PyException, _>(format!(
//             "Error converting arguments to kwargs to call client callback: {:?}",
//             e
//         ))
//     })?;

//     println!("Converted to Python kwargs_map: {:?}", kwargs_map);

//     // -> Convert to py dict
//     let kwargs = PyDict::new(py);
//     for (key, value) in kwargs_map {
//         kwargs.set_item(key, value).unwrap();
//     }

//     // Call the Python function with the converted arguments
//     let result = function.call(py, (), Some(kwargs)).map_err(|e| e)?;

//     let result_obj: PyObject = result.clone().into(); // Convert the result into a PyObject

//     Ok(result_obj) // Return the PyObject
// }
