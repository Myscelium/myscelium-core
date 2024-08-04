use std::collections::HashMap;
use std::fmt;

use crate::{CallbackClosure, CommandType, HandlerStatus};
use indexmap::IndexMap;

use serde_json::Value;

pub struct FunctionMetadata {
    pub name: &'static str,
    pub args: IndexMap<String, String>,
    pub func: CallbackClosure,
}

pub struct Callback {
    pub actf_name: String,
    pub callable: Box<CallbackClosure>,
    pub parameters: IndexMap<String, String>,
    pub callback_type: CommandType,
    pub status: HandlerStatus,
    pub response_structure: HashMap<String, Value>,
    pub description: String,
}

impl fmt::Debug for FunctionMetadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FunctionMetadata")
            .field("name", &self.name)
            .field("args", &self.args)
            // We exclude the `func` field as it is complex to print
            .finish()
    }
}

impl Callback {
    pub fn new(actf_name: String, callable: Box<CallbackClosure>, parameters: IndexMap<String, String>, callback_type: CommandType, status: HandlerStatus, response_structure: HashMap<String, Value>, description: String) -> Self {
        Self {
            actf_name,
            callable,
            parameters,
            callback_type,
            status,
            response_structure,
            description,
        }
    }
}

// "callback_name" {
//       "callback": Box<CallbackClosure>
//       "parameters": IndexMap<String, String>
//       "type": CallbackType
//       "status": HandlerStatus,
//       "response_structure": HashMap<String, Value>,
//       "description": String
// }
