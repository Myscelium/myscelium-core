// SPDX-License-Identifier: MPL-2.0
// Copyright © 2021-2026 Cristian Camargo Filho

use serde_json::Value;
use std::collections::HashMap;

use crate::common::functions::verifiers::{fast_json_comparator, ComparatorError};

use crate::common::client_manager::manager::{Client, ClientError};
use crate::common::enhanced_buffer::utilities::{
    CommandInstructions, CommandMode, CommandOrigin, CommandStatus, CommandTarget, CommandType,
};
use crate::socket_host::transposer_functions::handle_direct_function::ProcessResult;

macro_rules! create_error_response_and_return {
    ($error:expr) => {{
        let new_command_instructions = CommandInstructions::new(
            CommandMode::Response,
            CommandType::DirectFunction,
            CommandTarget::Origin,
            CommandStatus::Failure,
            CommandOrigin::Host,
            "error_handler".to_string(),
            HashMap::new(),
            $error.to_string(),
        );

        new_command_instructions
    }};
}

use crate::socket_host::host_logger::log_handler::Logger;
use crate::HOST_LOG_LEVEL;

macro_rules! acquire_logger {
    ($section_name:expr) => {{
        let host_log_level;
        {
            let log_level = HOST_LOG_LEVEL.lock();
            host_log_level = log_level.clone()
        }
        Logger::new(host_log_level, $section_name)
    }};
}

pub fn cast_new_client(new_client: &HashMap<String, Value>) -> Result<Client, ProcessResult> {
    let mut expected: HashMap<String, Value> = HashMap::new();

    expected.insert("client_name".to_string(), Value::String("".to_string()));
    expected.insert("client_key".to_string(), Value::String("".to_string()));
    expected.insert("client_type".to_string(), Value::String("".to_string()));
    expected.insert(
        "permission_group".to_string(),
        Value::String("".to_string()),
    );
    expected.insert("is_super_user".to_string(), Value::Bool(false));
    expected.insert(
        "max_sub_channels".to_string(),
        Value::Number(serde_json::Number::from(0)),
    );
    expected.insert("owned_sub_channels_keys".to_string(), Value::Array(vec![]));

    let parsed_new_client = fast_json_comparator(
        &Value::Object(serde_json::Map::from_iter(new_client.clone())),
        &Value::Object(serde_json::Map::from_iter(expected)),
    );

    let verified_client_value: Value = match parsed_new_client {
        Err(e) => match e {
            ComparatorError::TypeMismatch(tp) => {
                // logger.warn(format!("ERROR, Client kwargs have mismatch type {} kwarg!", tp));
                return Err(ProcessResult::Error(format!(
                    "Error! Client kwargs have mismatch type {} kwarg!",
                    tp
                )));
            }
            ComparatorError::LengthMismatch => {
                // logger.warn("ERROR, Client kwargs have mismatch relative length kwargs!".to_string());
                return Err(ProcessResult::Error(format!(
                    "Error! Client kwargs have mismatch relative length kwargs!"
                )));
            }
            ComparatorError::MissingKey(k) => {
                // logger.warn(format!("ERROR, Client kwargs have a missing kwarg: {}!", k));
                return Err(ProcessResult::Error(format!(
                    "Error! Client kwargs have a missing kwarg: {}!",
                    k
                )));
            }
            ComparatorError::TargetIsEmpty => {
                // logger.warn("ERROR, Client target pattern is empty!".to_string());
                return Err(ProcessResult::Error(format!(
                    "Error! Client target pattern is empty!"
                )));
            }
            ComparatorError::ParseError(e) => {
                // logger.warn(format!("ERROR, Can't parse {:?}!", e).to_string());
                return Err(ProcessResult::Error(format!(
                    "Error! Client target pattern is empty!"
                )));
            }
        },

        Ok(new_client) => new_client,
    };

    // let owned_sub_channels_keys: Vec<String> = verified_client_value.get("owned_sub_channels_keys").unwrap().as_array().unwrap().iter().map(|v| v.as_str().unwrap()).collect();

    let owned_sub_channels_keys_result: Result<Vec<String>, _> = verified_client_value
        .get("owned_sub_channels_keys")
        .ok_or("Key not found")
        .and_then(|v| v.as_array().ok_or("Not an array"))
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        });

    let owned_sub_channels_keys: Vec<String> = match owned_sub_channels_keys_result {
        Ok(keys) => keys,
        Err(e) => {
            return Err(ProcessResult::Error(format!(
                "Error! Can't extract new client owned_sub_channels_keys error: {}!",
                e
            )));
        }
    };

    let client_key = verified_client_value
        .get("client_key")
        .unwrap()
        .as_str()
        .map(|s| s.to_string())
        .unwrap();

    let client_handlers: Vec<HashMap<String, Value>> = Vec::new();

    // TODO >>> Create a better mechanism to unpack these kwargs from json and return errors when need!
    let new_client = Client::new(
        verified_client_value
            .get("client_name")
            .unwrap()
            .as_str()
            .map(|s| s.to_string())
            .unwrap(),
        client_key.clone(),
        verified_client_value
            .get("client_type")
            .unwrap()
            .as_str()
            .map(|s| s.to_string())
            .unwrap(),
        verified_client_value
            .get("permission_group")
            .unwrap()
            .as_str()
            .map(|s| s.to_string())
            .unwrap(),
        verified_client_value
            .get("is_super_user")
            .unwrap()
            .as_bool()
            .unwrap(),
        verified_client_value
            .get("max_sub_channels")
            .unwrap()
            .as_f64()
            .unwrap() as u32, // TODO >>> Create a better handler to cases greather than u32
        owned_sub_channels_keys,
        client_handlers,
    )
    .map_err(ProcessResult::from)?;

    // logger.debug(format!("New client: {:?}", new_client));

    Ok(new_client)
}
