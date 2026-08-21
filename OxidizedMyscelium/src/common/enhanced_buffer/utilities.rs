// SPDX-License-Identifier: MPL-2.0
// Copyright © 2021-2026 Cristian Camargo Filho

use crate::common::client_manager::manager::Client;
use crate::common::enhanced_buffer;
use crate::common::enhanced_buffer::buffer_down_manager::DownCommand;
use crate::common::enhanced_buffer::buffer_up_manager::UpCommand;
use serde::{Deserialize, Serialize};
use serde_json::{from_str, Map, Value};
use std::collections::HashMap;

use std::fmt;

trait Stringifiable: Sized {
    fn eq_str(&self, other: &str) -> bool;
}

// Implement the trait for each of your enums
macro_rules! impl_stringfiable_for_enum {
    ($($t:ty),+) => {
        $(
            impl Stringifiable for $t {
                fn eq_str(&self, other:&str) -> bool {
                    format!("{:?}", self) == other
                }
            }

            impl fmt::Display for $t {
                fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    write!(f, "{:?}", self)
                }
            }

            impl PartialEq<&str> for $t {
                fn eq(&self, other:&&str) -> bool {
                    self.eq_str(other)
                }
            }

        )+
    }
}

#[derive(Debug)]
pub enum CommandError {
    InvalidResponse(String),
    InvalidCommand(String),
    NotAJsonObject,
    DeserializationError(serde_json::Error),
}

impl From<serde_json::Error> for CommandError {
    fn from(err: serde_json::Error) -> Self {
        CommandError::DeserializationError(err)
    }
}

impl fmt::Display for CommandError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            CommandError::InvalidResponse(ref msg) => write!(f, "Invalid response: {}", msg),
            CommandError::InvalidCommand(ref msg) => write!(f, "Invalid Command: {}", msg),
            CommandError::NotAJsonObject => write!(f, "The value is not a JSON object!"),
            CommandError::DeserializationError(ref err) => {
                write!(f, "Deserialization error: {}", err)
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum CommandMode {
    Function,
    Response,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum CommandType {
    SpecialFunction,
    DirectFunction,
    ExternalFunction,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ResponseType {
    SpecialFunction,
    DirectFunction,
    ExternalFunction,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CommandStatus {
    Success,
    Failure,
}

// impl PartialEq<&str> for CommandStatus {
//     fn eq(&self, other: &&str) -> bool {
//         match self {
//             CommandStatus::Success => *other == "Success",
//             CommandStatus::Failure => *other == "Failure",
//         }
//     }
// }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CommandTarget {
    Origin,
    #[serde(rename = "ClientKey")]
    ClientKey(String),
    Host,
}

impl CommandTarget {
    pub fn as_pure_string(&self) -> String {
        match self {
            CommandTarget::ClientKey(key) => key.clone(),
            CommandTarget::Host => "Host".to_string(),
            CommandTarget::Origin => "Origin".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ResponseTarget {
    Origin,
    #[serde(rename = "ClientKey")]
    ClientKey(String),
    Host,
}

impl_stringfiable_for_enum!(
    CommandMode,
    CommandType,
    CommandStatus,
    CommandTarget,
    ResponseType,
    ResponseTarget
);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CommandOrigin {
    Host,
    #[serde(rename = "ClientKey")]
    ClientKey(String),
}

impl CommandOrigin {
    pub fn to_string(&self) -> String {
        match self {
            CommandOrigin::ClientKey(key) => key.clone(),
            CommandOrigin::Host => "Host".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandInstructions {
    pub mode: CommandMode,
    #[serde(rename = "type")]
    pub command_type: CommandType,
    pub target: CommandTarget,
    pub status: CommandStatus,
    pub origin: CommandOrigin,
    pub actf: String,
    pub kwargs: HashMap<String, serde_json::Value>,
    pub message: String,
    pub response_type: Option<ResponseType>,
    pub response_target: Option<ResponseTarget>,
    pub response_actf: Option<String>,
    pub collect_response: bool,
}

impl CommandInstructions {
    pub fn new(
        mode: CommandMode,
        command_type: CommandType,
        target: CommandTarget,
        status: CommandStatus,
        origin: CommandOrigin,
        actf: String,
        kwargs: HashMap<String, serde_json::Value>,
        message: String,
        response_type: Option<ResponseType>,
        response_target: Option<ResponseTarget>,
        response_actf: Option<String>,
        collect_response: bool,
    ) -> Self {
        Self {
            mode,
            command_type,
            target,
            status,
            origin,
            actf,
            kwargs,
            message,
            response_type,
            response_target,
            response_actf,
            collect_response,
        }
    }

    pub fn to_value_map(&self) -> Value {
        serde_json::to_value(&self).unwrap()
    }

    pub fn to_string_value(&self) -> HashMap<String, Value> {
        // Serialize the struct to a JSON string
        let json_str: String = serde_json::to_string(&self).unwrap();

        // Deserialize the JSON string into a HashMap
        serde_json::from_str(&json_str).unwrap()
    }

    pub fn convert_to_hashmap_string_value(&self) -> HashMap<String, Value> {
        let mut map = HashMap::new();

        map.insert(
            "mode".to_string(),
            serde_json::to_value(self.mode.clone()).unwrap(),
        );
        map.insert(
            "type".to_string(),
            serde_json::to_value(self.command_type.clone()).unwrap(),
        );
        map.insert(
            "target".to_string(),
            serde_json::to_value(self.target.clone()).unwrap(),
        );
        map.insert(
            "status".to_string(),
            serde_json::to_value(self.status.clone()).unwrap(),
        );
        map.insert(
            "origin".to_string(),
            serde_json::to_value(self.origin.clone()).unwrap(),
        );
        map.insert("actf".to_string(), Value::String(self.actf.clone()));
        map.insert("message".to_string(), Value::String(self.message.clone()));

        // Insert `kwargs` directly
        map.extend(self.kwargs.clone());

        map
    }

    pub fn from_value_map(mut map: HashMap<String, Value>) -> Result<Self, CommandError> {
        let mode = match map.get("mode").and_then(Value::as_str) {
            Some("Function") => CommandMode::Function,
            Some("Response") => CommandMode::Response,
            _ => {
                return Err(CommandError::InvalidCommand(
                    "Invalid or missing mode".to_string(),
                ))
            }
        };

        let command_type = match map.get("type").and_then(Value::as_str) {
            Some("SpecialFunction") => CommandType::SpecialFunction,
            Some("DirectFunction") => CommandType::DirectFunction,
            Some("ExternalFunction") => CommandType::ExternalFunction,
            _ => {
                return Err(CommandError::InvalidCommand(
                    "Invalid or missing type".to_string(),
                ))
            }
        };

        // let target = map.get("target").and_then(Value::as_str).map(String::from).ok_or_else(|| CommandError::InvalidCommand("Missing target".to_string()))?;

        let target = match map.get("target").and_then(Value::as_str) {
            Some("Host") => CommandTarget::Host,
            Some("Origin") => CommandTarget::Origin,
            Some(c) => {
                if c == "" {
                    return Err(CommandError::InvalidCommand(
                        "Invalid or missing target".to_string(),
                    ));
                }

                CommandTarget::ClientKey(c.to_string())
            }
            None => {
                return Err(CommandError::InvalidCommand(
                    "Invalid or missing target".to_string(),
                ));
            }
        };

        let status = match map.get("status").and_then(Value::as_str) {
            Some("Success") => CommandStatus::Success,
            Some("Failure") => CommandStatus::Failure,
            _ => {
                return Err(CommandError::InvalidCommand(
                    "Invalid or missing status".to_string(),
                ))
            }
        };

        let origin = match map.get("origin").and_then(Value::as_str) {
            Some("Host") => CommandOrigin::Host,
            Some(c) => {
                if c == "" {
                    return Err(CommandError::InvalidCommand(
                        "Invalid or missing response target".to_string(),
                    ));
                }

                CommandOrigin::ClientKey(c.to_string())
            }
            _ => {
                return Err(CommandError::InvalidCommand(
                    "Invalid or missing origin".to_string(),
                ))
            }
        };

        let actf = map
            .get("actf")
            .and_then(Value::as_str)
            .map(String::from)
            .ok_or_else(|| CommandError::InvalidCommand("Missing actf".to_string()))?;
        let message = map
            .get("message")
            .and_then(Value::as_str)
            .map(String::from)
            .ok_or_else(|| CommandError::InvalidCommand("Missing message".to_string()))?;

        // Extract kwargs directly
        let kwargs = map.remove("kwargs").map_or_else(
            || HashMap::new(),
            |v| {
                v.as_object().map_or_else(
                    || HashMap::new(),
                    |map| {
                        map.into_iter()
                            .map(|(k, v)| (k.clone(), v.clone()))
                            .collect()
                    },
                )
            },
        );

        // Extract response type:

        let response_type = match map.get("response_type").and_then(Value::as_str) {
            Some("SpecialFunction") => ResponseType::SpecialFunction,
            Some("DirectFunction") => ResponseType::DirectFunction,
            Some("ExternalFunction") => ResponseType::ExternalFunction,
            _ => {
                return Err(CommandError::InvalidCommand(
                    "Invalid or missing response type".to_string(),
                ))
            }
        };

        // Extract response target

        let response_target = match map.get("response_target").and_then(Value::as_str) {
            Some("Host") => ResponseTarget::Host,
            Some("Origin") => ResponseTarget::Origin,
            Some(c) => {
                if c == "" {
                    return Err(CommandError::InvalidCommand(
                        "Invalid or missing response target".to_string(),
                    ));
                }

                ResponseTarget::ClientKey(c.to_string())
            }
            None => {
                return Err(CommandError::InvalidCommand(
                    "Invalid or missing response target".to_string(),
                ));
            }
        };

        let response_actf = map
            .get("response_actf")
            .and_then(Value::as_str)
            .map(String::from)
            .ok_or_else(|| CommandError::InvalidCommand("Missing respnse_actf!".to_string()))?;

        let collect_response = map
            .get("collect_response")
            .and_then(Value::as_bool)
            .ok_or_else(|| CommandError::InvalidCommand("Missing response_actf!".to_string()))?;

        println!("Converted kwargs value object to Map: {:?}", kwargs);

        Ok(CommandInstructions {
            mode,
            command_type,
            target,
            status,
            origin,
            actf,
            kwargs,
            message,
            response_type: Some(response_type),
            response_target: Some(response_target),
            response_actf: Some(response_actf),
            collect_response,
        })
    }

    pub fn from_string_hashmap(mut map: HashMap<String, String>) -> Result<Self, CommandError> {
        let mode = match map.get("mode").map(String::as_str) {
            Some("Function") => CommandMode::Function,
            Some("Response") => CommandMode::Response,
            _ => {
                return Err(CommandError::InvalidCommand(
                    "Invalid or missing mode".to_string(),
                ))
            }
        };

        let command_type = match map.get("type").map(String::as_str) {
            Some("SpecialFunction") => CommandType::SpecialFunction,
            Some("DirectFunction") => CommandType::DirectFunction,
            Some("ExternalFunction") => CommandType::ExternalFunction,

            _ => {
                return Err(CommandError::InvalidCommand(
                    "Invalid or missing type".to_string(),
                ))
            }
        };

        // let target = map.get("target").cloned().ok_or_else(|| "Missing target".to_string())?;

        let target = match map.get("target").map(String::as_str) {
            Some("Host") => CommandTarget::Host,
            Some("Origin") => CommandTarget::Origin,
            Some(c) => {
                if c != "" {
                    // Check if the string matches the format "ClientKey(some_client_id)"
                    if c.starts_with("ClientKey(") && c.ends_with(")") {
                        let client_id = &c["ClientKey(".len()..c.len() - 1];
                        CommandTarget::ClientKey(client_id.to_string())
                    } else {
                        return Err(CommandError::InvalidCommand(
                            "Invalid or missing target".to_string(),
                        ));
                    }
                } else {
                    return Err(CommandError::InvalidCommand(
                        "Invalid or missing target".to_string(),
                    ));
                }
            }
            None => {
                return Err(CommandError::InvalidCommand(
                    "Invalid or missing target".to_string(),
                ));
            }
        };

        let status = match map.get("status").map(String::as_str) {
            Some("Success") => CommandStatus::Success,
            Some("Failure") => CommandStatus::Failure,
            _ => {
                return Err(CommandError::InvalidCommand(
                    "Invalid or missing status".to_string(),
                ))
            }
        };

        let origin = match map.get("origin").map(String::as_str) {
            Some("Host") => CommandOrigin::Host,
            Some(client_id) => {
                // Check if the string matches the format "ClientKey(some_client_id)"
                if client_id.starts_with("ClientKey(") && client_id.ends_with(")") {
                    let client_key = &client_id["ClientKey(".len()..client_id.len() - 1];
                    CommandOrigin::ClientKey(client_key.to_string())
                } else {
                    return Err(CommandError::InvalidCommand(
                        "Invalid or missing origin".to_string(),
                    ));
                }
            }
            _ => {
                return Err(CommandError::InvalidCommand(
                    "Invalid or missing origin".to_string(),
                ))
            }
        };

        let actf = map
            .get("actf")
            .cloned()
            .ok_or_else(|| CommandError::InvalidCommand("Missing actf".to_string()))?;
        let message = map
            .get("message")
            .cloned()
            .ok_or_else(|| CommandError::InvalidCommand("Missing message".to_string()))?;

        // Extract and parse the kwargs field
        let kwargs = if let Some(kwargs_str) = map.remove("kwargs") {
            serde_json::from_str::<HashMap<String, Value>>(&kwargs_str).map_err(|e| {
                CommandError::InvalidCommand(format!("Failed to parse kwargs: {}", e))
            })?
        } else {
            HashMap::new()
        };

        // Extract response_type:

        let response_type = match map.get("response_type").map(String::as_str) {
            Some("SpecialFunction") => ResponseType::SpecialFunction,
            Some("DirectFunction") => ResponseType::DirectFunction,
            Some("ExternalFunction") => ResponseType::ExternalFunction,

            _ => {
                return Err(CommandError::InvalidCommand(
                    "Invalid or missing response type".to_string(),
                ))
            }
        };

        // Extract response_target:

        let response_target = match map.get("response_target").map(String::as_str) {
            Some("Host") => ResponseTarget::Host,
            Some("Origin") => ResponseTarget::Origin,
            Some(c) => {
                if c != "" {
                    // Check if the string matches the format "ClientKey(some_client_id)"
                    if c.starts_with("ClientKey(") && c.ends_with(")") {
                        let client_id = &c["ClientKey(".len()..c.len() - 1];
                        ResponseTarget::ClientKey(client_id.to_string())
                    } else {
                        return Err(CommandError::InvalidCommand(
                            "Invalid or missing response target!".to_string(),
                        ));
                    }
                } else {
                    return Err(CommandError::InvalidCommand(
                        "Invalid or missing response target!".to_string(),
                    ));
                }
            }
            None => {
                return Err(CommandError::InvalidCommand(
                    "Invalid or missing response target!".to_string(),
                ));
            }
        };

        let response_actf = map
            .get("response_actf")
            .cloned()
            .ok_or_else(|| CommandError::InvalidCommand("Missing response actf".to_string()))?;

        let collect_response = map
            .get("collect_response")
            .and_then(|s| match s.as_str() {
                "1" => Some(true),
                "0" => Some(false),
                _ => None,
            })
            .ok_or_else(|| {
                CommandError::InvalidCommand("Missing or invalid collect_response!".to_string())
            })?;

        // TODO >>> See if the response target and the response actf need to have a None parser

        Ok(CommandInstructions {
            mode,
            command_type,
            target,
            status,
            origin,
            actf,
            kwargs,
            message,
            response_type: Some(response_type),
            response_target: Some(response_target),
            response_actf: Some(response_actf),
            collect_response,
        })
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Command {
    pub client_key: String,
    pub parity_id: String,
    pub priority: u8,
    pub command: CommandInstructions,
}

fn transform_value(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut new_map = HashMap::new();
            for (key, val) in map.iter() {
                // Iterating over references
                if let Some(inner_val) = val.get("Map") {
                    new_map.insert(key.clone(), transform_value(inner_val));
                } else if let Some(inner_val) = val.get("List") {
                    new_map.insert(key.clone(), transform_value(inner_val));
                } else if let Some(Value::String(s)) = val.get("Str") {
                    new_map.insert(key.clone(), Value::String(s.clone()));
                } else if let Value::Object(_) = val {
                    new_map.insert(key.clone(), transform_value(val));
                } else {
                    new_map.insert(key.clone(), transform_value(val));
                }
            }
            Value::Object(serde_json::Map::from_iter(new_map)) // Convert HashMap to serde_json::Map using into()
        }
        Value::String(s) => {
            // If the string is a JSON representation, parse it and transform it
            if let Ok(parsed) = serde_json::from_str::<Value>(s) {
                transform_value(&parsed)
            } else {
                Value::String(s.clone())
            }
        }
        Value::Array(arr) => Value::Array(arr.iter().map(|v| transform_value(v)).collect()),
        _ => value.clone(),
    }
}

impl Command {
    pub fn new(
        client_key: String,
        parity_id: String,
        priority: u8,
        command: CommandInstructions,
    ) -> Self {
        Self {
            client_key,
            parity_id,
            priority,
            command,
        }
    }

    /// Converts a `DownCommand` into `Command`.
    ///
    /// This function takes a `DownCommand` and attempts to convert it into an instance of `YourStruct`.
    /// It extracts the `client_key`, `parity_id`, and `priority` fields from the `DownCommand`,
    /// and then tries to deserialize the `command` JSON string into `CommandInstructions`.
    ///
    /// # Arguments
    /// * `down_command` - A `DownCommand` instance containing the necessary information.
    ///
    /// # Returns
    /// * `Ok(YourStruct)` - If the conversion is successful.
    /// * `Err(CommandError::InvalidCommand)` - If the `command` field cannot be parsed into `CommandInstructions`.
    ///
    /// # Examples
    /// ```
    /// // Example usage
    /// let down_command = DownCommand {
    ///     client_key: "client_key_value".to_string(),
    ///     parity_id: "parity_id_value".to_string(),
    ///     priority: "priority_value".to_string(),
    ///     command: "{\"mode\":\"default\", ...}".to_string(),
    /// };
    /// let result = YourStruct::from_down_command(down_command);
    /// ```
    pub fn from_down_command(down_command: &DownCommand) -> Result<Self, CommandError> {
        let client_key = down_command.client_key.clone();
        let parity_id = down_command.parity_id.clone();
        let priority = down_command.priority.clone();

        let command: CommandInstructions = match serde_json::from_str(&down_command.command.clone())
        {
            Ok(c) => c,
            Err(_) => return Err(CommandError::InvalidCommand("".to_string())),
        };

        Ok(Self {
            client_key,
            parity_id,
            priority,
            command,
        })
    }

    /// Converts an `UpCommand` into `Command`.
    ///
    /// This function takes an `UpCommand` and attempts to convert it into an instance of `YourStruct`.
    /// It extracts the `client_key`, `parity_id`, and `priority` fields from the `UpCommand`,
    /// and then tries to deserialize the `command` JSON string into `CommandInstructions`.
    ///
    /// # Arguments
    /// * `up_command` - An `UpCommand` instance containing the necessary information.
    ///
    /// # Returns
    /// * `Ok(YourStruct)` - If the conversion is successful.
    /// * `Err(CommandError::InvalidCommand)` - If the `command` field cannot be parsed into `CommandInstructions`.
    ///
    /// # Examples
    /// ```
    /// // Example usage
    /// let up_command = UpCommand {
    ///     client_key: "client_key_value".to_string(),
    ///     parity_id: "parity_id_value".to_string(),
    ///     priority: "priority_value".to_string(),
    ///     command: "{\"mode\":\"default\", ...}".to_string(),
    /// };
    /// let result = YourStruct::from_up_command(up_command);
    /// ```
    pub fn from_up_command(up_command: &UpCommand) -> Result<Self, CommandError> {
        let client_key = up_command.client_key.clone();
        let parity_id = up_command.parity_id.clone();
        let priority = up_command.priority.clone();

        let command: CommandInstructions = match serde_json::from_str(&up_command.command.clone()) {
            Ok(c) => c,
            Err(e) => {
                return Err(CommandError::InvalidCommand(format!(
                    "The error is: {:?}",
                    e
                )))
            }
        };

        println!("Client -> Command from UpCommand: {:?}", command);

        Ok(Self {
            client_key,
            parity_id,
            priority,
            command,
        })
    }

    pub fn command_type(&self) -> CommandType {
        return self.command.command_type.clone();
    }

    /// Converts a Command instance into a HashMap<String, Value>
    pub fn command_to_hashmap(&self) -> Result<HashMap<String, Value>, CommandError> {
        let mut map = Map::new();

        // Serialize simple fields directly
        map.insert(
            "client_key".to_owned(),
            serde_json::Value::from(self.client_key.clone()),
        );
        map.insert(
            "parity_id".to_owned(),
            serde_json::Value::from(self.parity_id.clone()),
        );
        map.insert(
            "priority".to_owned(),
            serde_json::Value::from(serde_json::Number::from_f64(self.priority as f64).unwrap()),
        );

        // For the command field, use its to_string_value method
        let command_map = &self.command.to_value_map();
        map.insert("command".to_owned(), command_map.clone());

        // Convert the serde_json Map to HashMap
        Ok(map.into_iter().collect())
    }
}

#[derive(Debug, Clone)]
pub enum CommandVariant {
    Command(Command),
    UpCommand(UpCommand),
    DownCommand(DownCommand),
}
