use crate::common::enhanced_buffer::utilities::CommandInstructions;
use crate::common::enhanced_buffer::utilities::CommandType;
use crate::common::structs::results_structs::ResultType;

use std::collections::HashMap;
use std::result;
use std::sync::MutexGuard;

use crate::common::enhanced_buffer::utilities::Command;

use serde_json::{json, Value};

use serde_json::Value as JsonValue;

/// Converts a JSON value to its corresponding Python object.
///
/// This helper function takes in a JSON value and recursively converts it to the corresponding Python object.
/// This can be useful for translating between Rust and Python data structures.
///
/// # Parameters
///
/// - `py`: The Python interpreter.
/// - `value`: The JSON value to convert.
///
/// # Returns
///
/// Returns the converted Python object.
pub fn translate_value_to_py(py: Python<'_>, value: JsonValue) -> PyResult<PyObject> {
    // Convert the JSON value to the appropriate Python object
    match value {
        JsonValue::Null => Ok(py.None()),
        JsonValue::Bool(b) => Ok(b.into_py(py)),
        JsonValue::Number(num) => Ok(num.as_f64().unwrap().into_py(py)),
        JsonValue::String(s) => Ok(s.into_py(py)),
        JsonValue::Array(arr) => {
            let py_list = PyList::empty(py);
            for item in arr {
                let py_item = translate_value_to_py(py, item)?;
                py_list.append(py_item)?;
            }
            Ok(py_list.into())
        }
        JsonValue::Object(obj) => {
            let py_dict: &PyDict = PyDict::new(py);
            for (k, v) in obj {
                let py_key = k.into_py(py);
                let py_value = translate_value_to_py(py, v)?;
                py_dict.set_item(py_key, py_value)?;
            }
            Ok(py_dict.into())
        }
    }
}

pub fn extract_arg_types(arg: &PyAny) -> PyResult<Value> {
    if let Ok(arg_dict) = arg.downcast::<PyDict>() {
        // If the argument is a dictionary, recursively extract the argument types
        let mut args_types = HashMap::new();
        for (arg_name, arg_type) in arg_dict.iter() {
            let arg_name: String = arg_name.extract()?;
            let arg_type_value = extract_arg_types(arg_type)?;
            args_types.insert(arg_name, arg_type_value);
        }
        Ok(json!(args_types))
    } else {
        // If the argument is not a dictionary, extract it as a string
        let arg_type: String = arg.extract()?;
        Ok(json!(arg_type))
    }
}

pub fn json_value_to_py_object(py: Python, value: &Value) -> PyResult<PyObject> {
    match value {
        Value::Null => Ok(py.None()),
        Value::Bool(b) => Ok(b.into_py(py)),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(i.into_py(py))
            } else if let Some(f) = n.as_f64() {
                Ok(f.into_py(py))
            } else {
                Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "Invalid number type",
                ))
            }
        }
        Value::String(s) => Ok(s.clone().into_py(py)),
        Value::Array(arr) => {
            let py_list = PyList::new(
                py,
                arr.iter().map(|v| json_value_to_py_object(py, v).unwrap()),
            );
            Ok(py_list.into())
        }
        Value::Object(obj) => {
            let py_dict = PyDict::new(py);
            for (k, v) in obj {
                py_dict.set_item(k, json_value_to_py_object(py, v)?.to_object(py))?;
            }
            Ok(py_dict.into())
        }
    }
}

pub fn dict_to_kwargs<'l>(
    py: Python<'l>,
    dict: &HashMap<String, Value>,
) -> PyResult<HashMap<String, PyObject>> {
    let mut kwargs: HashMap<String, PyObject> = HashMap::new();
    for (key, value) in dict.iter() {
        let py_value = json_value_to_py_object(py, value)?;
        kwargs.insert(key.clone(), py_value);
    }

    Ok(kwargs)
}

pub fn dict_to_tuple<'l>(py: Python<'l>, dict: &HashMap<String, Value>) -> PyResult<&'l PyTuple> {
    // let logger = acquire_logger!("Transposer - Py Dict to Tuple Converter");

    // Check if the dict contains the function name as a key
    if !dict.contains_key("kwargs") {
        // If it does not, return an empty Vec since there are no arguments
        let mut values: Vec<PyObject> = Vec::new();
        return Ok(PyTuple::new(py, values));
    }

    let args_string = match dict.get("kwargs") {
        Some(Value::String(s)) => s,
        _ => {
            return Err(PyErr::new::<PyException, _>(
                "The kwargs key is not found or not a string.",
            ))
        }
    };

    let sub_dict: HashMap<String, Value> = serde_json::from_str(args_string).unwrap();

    // logger.debug(format!("Args extracted: {:?}", sub_dict));

    let mut values: Vec<PyObject> = Vec::new();
    for value in sub_dict.values() {
        let py_value = match value {
            Value::String(s) => s.into_py(py),
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    i.into_py(py)
                } else if let Some(f) = n.as_f64() {
                    f.into_py(py)
                } else {
                    return Err(PyErr::new::<PyException, _>("Unsupported number type."));
                }
            }
            Value::Bool(b) => b.into_py(py),
            _ => return Err(PyErr::new::<PyException, _>("Unsupported value type.")),
        };
        values.push(py_value);
    }

    let py_tuple = PyTuple::new(py, &values);

    // logger.debug(format!("py_tuple: {}", py_tuple));

    Ok(py_tuple)
}

pub fn extract_pyobject(py: Python, obj: PyObject) -> serde_json::Value {
    if let Ok(dict) = obj.cast_as::<PyDict>(py) {
        let mut rust_dict: HashMap<String, serde_json::Value> = HashMap::new();

        for (key, value) in dict.iter() {
            let key_str = match key.extract::<String>() {
                Ok(k) => k,
                Err(e) => {
                    println!("Failed to extract key as string: {:?}", e);
                    continue; // Skip this key-value pair
                }
            };

            if let Ok(value_str) = value.extract::<String>() {
                rust_dict.insert(key_str, Value::String(value_str));
            } else if let Ok(value_int) = value.extract::<i64>() {
                rust_dict.insert(key_str, Value::Number(value_int.into()));
            } else if let Ok(value_list) = value.cast_as::<PyList>() {
                let rust_list: Vec<_> = value_list
                    .iter()
                    .map(|item| extract_pyobject(py, item.to_object(py)))
                    .collect();
                rust_dict.insert(key_str, Value::Array(rust_list));
            } else if let Ok(nested_dict) = value.cast_as::<PyDict>() {
                rust_dict.insert(key_str, extract_pyobject(py, nested_dict.into()));
            } else {
                println!("Unmatched type for key: {}", key_str);
                // You may decide how to handle other types
            }
        }

        Value::Object(serde_json::Map::from_iter(rust_dict))
    } else if let Ok(tuple) = obj.cast_as::<PyTuple>(py) {
        let rust_list: Vec<_> = tuple
            .iter()
            .map(|item| extract_pyobject(py, item.to_object(py)))
            .collect();
        Value::Array(rust_list)
    } else if let Ok(list) = obj.cast_as::<PyList>(py) {
        let rust_list: Vec<_> = list
            .iter()
            .map(|item| extract_pyobject(py, item.to_object(py)))
            .collect();
        Value::Array(rust_list)
    } else if let Ok(int) = obj.cast_as::<PyInt>(py) {
        match int.extract::<i64>() {
            Ok(i) => {
                let num = serde_json::Number::from(i);
                Value::Number(num)
            }
            Err(e) => {
                println!("Failed to extract integer: {:?}", e);
                Value::Null
            }
        }
    } else if let Ok(float) = obj.cast_as::<PyFloat>(py) {
        match float.extract::<f64>() {
            Ok(i) => {
                if let Some(num) = serde_json::Number::from_f64(i) {
                    Value::Number(num)
                } else {
                    println!("Failed to extract float!");
                    Value::Null
                }
            }
            Err(e) => {
                println!("Failed to extract float: {:?}", e);
                Value::Null
            }
        }
    } else if let Ok(string) = obj.cast_as::<PyString>(py) {
        match string.extract() {
            Ok(s) => Value::String(s),
            Err(e) => {
                println!("Failed to extract string: {:?}", e);
                Value::Null
            }
        }
    } else if let Ok(boolean) = obj.cast_as::<PyBool>(py) {
        match boolean.extract() {
            Ok(b) => Value::Bool(b),
            Err(e) => {
                println!("Failed to extract boolean: {:?}", e);
                Value::Null
            }
        }
    } else if obj.is_none(py) {
        Value::Null
    } else {
        println!("Unmatched type for object: {:?}", obj);
        Value::Null
    }
}

pub fn call_callback(
    py: Python<'_>,
    command: Command,
    callback_patterns: MutexGuard<'_, HashMap<String, (Py<PyFunction>, Value)>>,
) -> PyResult<PyObject> {
    println!("Command to call a callback: {:?}", command);

    let function_name: &String = &command.command.actf;

    // Get the function and args_types from the CALLBACK_PATTERNS
    let (function, _) = callback_patterns.get(function_name).unwrap();

    let command: &CommandInstructions = &command.command;

    let inner_hash_map: HashMap<_, _> = command.kwargs.clone().into_iter().collect();
    let kwargs_map: HashMap<String, Py<PyAny>> =
        dict_to_kwargs(py, &inner_hash_map).map_err(|e| {
            PyErr::new::<PyException, _>(format!(
                "Error converting arguments to kwargs to call client callback: {:?}",
                e
            ))
        })?;

    // let kwargs_map = match command.command_type() {
    //     CommandType::Response(_) => {
    //         let command = &command.command;

    //         let inner_hash_map: HashMap<_, _> = command.kwargs.clone().into_iter().collect();
    //         dict_to_kwargs(py, &inner_hash_map).map_err(|e| PyErr::new::<PyException, _>(format!("Error converting arguments to kwargs to call client callback: {:?}", e)))?
    //     },
    //     CommandType::Function(_) => {
    //         let command = &command.command;

    //         let inner_hash_map: HashMap<_, _> = command.kwargs.clone().into_iter().collect();
    //         dict_to_kwargs(py, &inner_hash_map).map_err(|e| PyErr::new::<PyException, _>(format!("Error converting arguments to kwargs to call client callback: {:?}", e)))?
    //     },

    //     _ => dict_to_kwargs(py, &command.command).map_err(|e| PyErr::new::<PyException, _>(format!("Error converting arguments to kwargs to call client callback: {:?}", e)))?,
    // };

    println!("Converted to Python kwargs_map: {:?}", kwargs_map);

    // -> Convert to py dict
    let kwargs = PyDict::new(py);
    for (key, value) in kwargs_map {
        kwargs.set_item(key, value).unwrap();
    }

    // Call the Python function with the converted arguments
    let result = function.call(py, (), Some(kwargs)).map_err(|e| e)?;

    let result_obj: PyObject = result.clone().into(); // Convert the result into a PyObject

    Ok(result_obj) // Return the PyObject
}

pub fn client_call_callback(
    py: Python<'_>,
    command: &Command,
    callback_patterns: &HashMap<std::string::String, (pyo3::Py<PyFunction>, serde_json::Value)>,
) -> PyResult<PyObject> {
    println!("Command to call a callback: {:?}", command);

    let function_name: &String = &command.command.actf;

    // Get the function and args_types from the CALLBACK_PATTERNS
    let (function, _) = callback_patterns.get(function_name).unwrap();

    let command: &CommandInstructions = &command.command;

    let inner_hash_map: HashMap<String, Value> = command.convert_to_hashmap_string_value();

    let mut kwargs_dict: HashMap<String, Value> = HashMap::new();

    kwargs_dict.insert(
        "data".to_string(),
        Value::Object(serde_json::Map::from_iter(inner_hash_map)),
    );

    let kwargs_map: HashMap<String, Py<PyAny>> = dict_to_kwargs(py, &kwargs_dict).map_err(|e| {
        PyErr::new::<PyException, _>(format!(
            "Error converting arguments to kwargs to call client callback: {:?}",
            e
        ))
    })?;

    println!("Converted to Python kwargs_map: {:?}", kwargs_map);

    // -> Convert to py dict
    let kwargs = PyDict::new(py);
    for (key, value) in kwargs_map {
        kwargs.set_item(key, value).unwrap();
    }

    // Call the Python function with the converted arguments
    let result = function.call(py, (), Some(kwargs)).map_err(|e| e)?;

    let result_obj: PyObject = result.clone().into(); // Convert the result into a PyObject

    Ok(result_obj) // Return the PyObject
}
