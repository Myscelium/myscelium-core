use std::collections::HashMap;

use crate::{CallbackClosure, CommandType, HandlerStatus};
use indexmap::IndexMap;

use serde_json::Value;

pub struct Callback {
    pub actf_name: String,
    pub callable: Box<CallbackClosure>,
    pub parameters: IndexMap<String, String>,
    pub callback_type: CommandType,
    pub status: HandlerStatus,
    pub response_structure: HashMap<String, Value>,
    pub description: String,
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
