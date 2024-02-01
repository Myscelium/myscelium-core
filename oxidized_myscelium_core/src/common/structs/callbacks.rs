use crate::common::functions::callbacks::call_callback;
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
    pub map: Arc<Mutex<HashMap<&'static str, CallbackClosure>>>,
}

#[derive(Clone, Debug, Serialize)]
pub enum CallbackError {
    CallbackDoNotExist(String),
    UnsupportedArgument(String),
    UnsupportedResponseArgument(String),
}

fn convert_json_value_to_any(value: &Value) -> Option<Box<dyn Any>> {
    match value {
        Value::String(s) => Some(Box::new(s.clone()) as Box<dyn Any>),
        Value::Number(n) if n.is_i64() => n.as_i64().map(|i| Box::new(i) as Box<dyn Any>),
        // Handle other types as needed
        _ => None,
    }
}

impl MyCallbacks {
    pub fn new() -> Self {
        MyCallbacks {
            map: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn insert(&self, key: &'static str, closure: CallbackClosure) {
        self.map.lock().insert(key, closure);
    }

    pub fn call(
        &self,
        key: &str,
        kwargs: HashMap<String, Value>,
    ) -> Result<Box<dyn Any>, CallbackError> {
        let map = self.map.lock();
        if let Some(closure) = map.get(key) {
            //>----------------------------------------------------------------------------
            //> EXTRACT ARGS
            let mut args: Vec<Box<dyn Any>> = Vec::new();
            for (_key, value) in kwargs {
                if let Some(any) = convert_json_value_to_any(&value) {
                    args.push(any);
                } else {
                    return Err(CallbackError::UnsupportedResponseArgument(_key));
                }
            }
            // Convert Box<dyn Any> to &[&dyn Any] for callback
            let args_refs: Vec<&dyn Any> = args.iter().map(|arg| &**arg).collect();
            Ok(closure(args))
        } else {
            Err(CallbackError::CallbackDoNotExist(key.to_string()))
        }
    }
}
