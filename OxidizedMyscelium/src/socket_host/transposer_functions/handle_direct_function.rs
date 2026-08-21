// SPDX-License-Identifier: MPL-2.0
// Copyright © 2021-2026 Cristian Camargo Filho

use crate::chrono::TimeZone;
use crate::common::client_manager::manager::get_all_clients;
use crate::common::client_manager::manager::{check_if_client_key_exists, Client, ClientError};
use crate::common::enhanced_buffer;
use crate::common::enhanced_buffer::utilities::{
    Command, CommandInstructions, CommandMode, CommandOrigin, CommandStatus, CommandTarget,
    CommandType, ResponseTarget, ResponseType,
};
use crate::common::functions::converters::{
    convert_json_map_to_hash_map, convert_value_map_to_resulttype_map, ConversionError,
};
use crate::common::structs::available_commands::NodeStatus;
use crate::common::structs::available_commands::{CommandPatterns, Node};
use crate::common::structs::results_structs::ResultType;
use crate::socket_client::transposer::ProcessError;
use crate::socket_host::functions::sync_analiser::{sync_verifier, SyncVerifierError};
use crate::socket_host::host_logger::log_handler::Logger;
use crate::socket_host::sync_controller::controller::Clients;
use crate::socket_host::transposer_functions::helpers::cast_new_client;
use crate::CLIENTS_SYNC_CONTROLLER;
use crate::HOST_COMMAND_PATTERNS;
use crate::HOST_LOG_LEVEL;
use chrono::Duration;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::hash::Hash;

macro_rules! acquire_logger {
    ($section_name:expr) => {{
        let host_log_level;
        {
            let log_level = HOST_LOG_LEVEL.lock().await;
            host_log_level = log_level.clone()
        }
        Logger::new(host_log_level, $section_name).await
    }};
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ProcessResult {
    Empty,
    List(Vec<ProcessResult>),
    Error(String),
    CommandInstructions(CommandInstructions),
}

impl From<ClientError> for ProcessResult {
    fn from(value: ClientError) -> ProcessResult {
        match value {
            ClientError::ClientDoesNotExist(client) => ProcessResult::Error(format!("Client: {:?} does not exist!", client)),
            ClientError::ClientAlreadyExist(client) => ProcessResult::Error(format!("Client: {:?} already exist!", client)),
            ClientError::UnexpectedError(e) => ProcessResult::Error(format!("Unexpected error: {:?}", e)),
            ClientError::NotAbleToReadClientStates => ProcessResult::Error(format!("Error, not able to read client states")),
            ClientError::ClientIsNotRunning(_) => unreachable!("ClientError::ClientIsNotRunning should never be converted into ClientLoaderError!"),
            ClientError::InvalidCommand(e) => unreachable!("ClientError::InvalidCommand ({:?}) should never be converted into ClientLoaderError!", e),
            ClientError::ClientIsNotFullyInitialized(_) => unreachable!("ClientError::ClientNotFullyInitialized should never be converted into ClientLoaderError!"),
            ClientError::TargetDoesntExists(_) => unreachable!("ClientError::TargetDoesntExists should never be converted into ClientLoaderError!"),
            ClientError::HandlerDoesntExist(_) => unreachable!("ClientError::HandlerDoesntExist should never be converted into ClientLoaderError!"),
            ClientError::ResponseHandlerDoesntExist(_) => unreachable!("ClientError::ResponseHandlerDoesntExist should never be converted into ClientLoaderError!"),
            ClientError::CantScheduleCommandsToItself => unreachable!("ClientError::CantScheduleCommandsToItself should never be converted into ClientLoaderError!"),
            ClientError::HostCantSendResponseToItself => unreachable!("ClientError::HostCantSendResponseToItself should never be converted into ClientLoaderError!"),
            ClientError::TargetCantSendResponseToItself => unreachable!("ClientError::TargetCantSendResponseToItself should never be converted into ClientLoaderError!"),
            ClientError::BufferError(e) => unreachable!("ClientError::BufferError {:?} should never be converted into ClientLoaderError!", e),
        }
    }
}

impl From<SyncVerifierError> for ProcessResult {
    fn from(value: SyncVerifierError) -> ProcessResult {
        match value {
            SyncVerifierError::Error(e) => ProcessResult::Error(e),
        }
    }
}

pub async fn handle_direct_function(
    client_key: &String,
    activation_key: &String,
    command: CommandInstructions,
    command_id: Option<u32>,
) -> ProcessResult {
    let logger = acquire_logger!("Transposer - Process - Handle Direct Functions");
    logger.info(format!("Initializing processing!")).await;
    logger
        .debug(format!(
            "function received in handle direct function: {}",
            activation_key
        ))
        .await;

    // -> ----------------------------------------------------------------------------------------------------------------------------------
    // -> SYNCRONIZATION MECHANISM

    match activation_key.as_str() {
        "get_registered_commands" => {
            logger
                .info(format!("Receive get_registered_commands in host!"))
                .await;

            // Lock the HOST_COMMAND_PATTERNS and insert the new map

            let actual_patterns;

            {
                actual_patterns = HOST_COMMAND_PATTERNS.lock().await.clone();
            }

            // -> get the client by the client key
            //let client = match Client::get_by_key(client_key) {
            //    Ok(c) => c,
            //    Err(e) => match e {
            //        ClientError::ClientDoesNotExist(_) => {
            //            return ProcessResult::Error(format!("Unknow client_key: {:?}", client_key));
            //        },
            //        _ => {
            //            return ProcessResult::Error(format!("Get a error {:?}, obtaining client: {:?}", e, client_key));
            //        },
            //    },
            //};

            let nodes: Vec<Node> = actual_patterns.get_all_nodes_except_node_with_key(client_key);
            let mut filtered_commands: HashMap<String, Value> = HashMap::new();
            filtered_commands.insert(
                "network_nodes".to_string(),
                serde_json::to_value(nodes).unwrap(),
            );

            logger
                .info(format!(
                    "Successfully actualize the host available commands!"
                ))
                .await;

            // enhanced_buffer::buffer_down_manager::buffer_down_remove_schedule_by_id(command_id.clone());

            let function: String = "update_available_host_commands".to_string();

            let new_command_instructions = CommandInstructions::new(
                CommandMode::Response,
                CommandType::ExternalFunction,
                CommandTarget::Origin,
                CommandStatus::Success,
                CommandOrigin::Host,
                function,
                filtered_commands,
                "".to_string(),
                Some(ResponseType::DirectFunction),
                Some(ResponseTarget::Host),
                None, // Not required in this case
                true,
            );

            return ProcessResult::CommandInstructions(new_command_instructions);
        }
        "update_client_commands_ref" => {
            // TODO >>> This should't anymore trigger sync to other clients nor send the sync commands to the other clients

            //> The client's that depends on this node that is being updated will be changed to sync = false by the sync_analiser
            //> This will only update this node and change it to sync, also will mark the sync = true signalising that this node is now sync

            //> The update of the known network by the client will be defined in the scheduler, when sended it will be updtaed
            //>
            //>

            logger
                .info(format!("Receive update_client_commands_ref in host!"))
                .await;

            // -> get the client by the client key
            let client = match Client::get_by_key(client_key).await {
                Ok(c) => c,
                Err(e) => match e {
                    ClientError::ClientDoesNotExist(_) => {
                        return ProcessResult::Error(format!(
                            "Unknow client_key: {:?}",
                            client_key
                        ));
                    }
                    _ => {
                        return ProcessResult::Error(format!(
                            "Get a error {:?}, obtaining client: {:?}",
                            e, client_key
                        ));
                    }
                },
            };

            logger
                .debug(format!(
                    "Prepared, the client thought retrieving for the sync status update"
                ))
                .await;

            let client_handlers; // Client Handlers contain a Value wrapped Node

            // Check if 'client_handlers' exists within 'kwargs'
            if let Some(handlers) = command.kwargs.get("client_handlers") {
                client_handlers = handlers;
            } else {
                return ProcessResult::Error(format!("update_client_commands_ref give the followign error: The 'client_handlers' key does not exist within 'kwargs'."));
            }

            logger.debug(format!("Extracted the client handlers")).await;

            // } else {
            //     return ResultType::Error(format!("update_client_commands_ref command doesn't have kwargs in it!"));
            // }

            let mut client_name: String = client.get_client_name();

            {
                let mut actual_patterns = HOST_COMMAND_PATTERNS.lock().await;
                let mut client_node = match Node::from_value(client_handlers.clone()) {
                    Ok(n) => n,
                    Err(e) => {
                        return ProcessResult::Error(format!(
                            "Error creating node, the error was: {:?}",
                            e
                        ));
                    }
                };

                logger.debug(format!("Obtained the client node")).await;

                client_node.change_node_status(NodeStatus::Online);
                actual_patterns.add_or_update_if_exists(client_node);

                logger
                    .debug(format!("Updated the client node status"))
                    .await;

                // client.update_handlers(client_node.get_node_handlers().unwrap()); // TODO >> Update the type of the hanlders to client
                match client
                    .change_sync_to(true)
                    .await
                    .map_err(|e| ProcessResult::from(e))
                {
                    Ok(_) => {}
                    Err(e) => {
                        return e;
                    }
                };

                match client
                    .save_into_db()
                    .await
                    .map_err(|e| ProcessResult::from(e))
                {
                    Ok(_) => {}
                    Err(e) => {
                        return e;
                    }
                };

                logger
                    .debug(format!(
                        "Updated the client in the database with the new status"
                    ))
                    .await;
            }

            {
                let mut controller = CLIENTS_SYNC_CONTROLLER.lock().await;
                let status = controller.update_client_sync_status(client_key, true);
            }

            let mut responses: Vec<ProcessResult> = Vec::new();

            // logger.info(format!("Receive client: {} handlers, retransmitting to: {:?}", client_key, clients).to_string());

            // TODO >>> The trigerring client remais without sync, this is a issue cause by the case where the last client that connects
            // Get remains without an update about the other nodes, the info sended to him is the last one the one were all the other clients
            // are with status require sync because the action of this triggering client connects make the other clients that have access to
            // it become not sync, to solve this is necessary to create a delay mechanism that can send this new info for this client when the
            // other ones finishes sync.

            // Generate confirmation to triggering client
            let new_command_instructions = CommandInstructions::new(
                CommandMode::Response,
                CommandType::SpecialFunction,
                CommandTarget::Origin,
                CommandStatus::Success,
                CommandOrigin::Host,
                "C210".to_string(),
                HashMap::new(),
                "".to_string(),
                None, // Not required in this case
                None, // Not required in this case
                None, // Not required in this case
                true,
            );

            // > Verify the nodes that needs to be notified of this update in this client node (restrictivety without cause waves of unecessary updates)
            match sync_verifier().await.map_err(|e| ProcessResult::from(e)) {
                Ok(_) => {}
                Err(e) => return e,
            };

            return ProcessResult::CommandInstructions(new_command_instructions);
        }

        "restrictive_update_client_commands_ref" => {
            logger
                .info(format!(
                    "Receive restrictive_update_client_commands_ref in host!"
                ))
                .await;

            // -> get the client by the client key
            let client = match Client::get_by_key(client_key).await {
                Ok(c) => c,
                Err(e) => match e {
                    ClientError::ClientDoesNotExist(_) => {
                        return ProcessResult::Error(format!(
                            "Unknow client_key: {:?}",
                            client_key
                        ));
                    }
                    _ => {
                        return ProcessResult::Error(format!(
                            "Get a error {:?}, obtaining client: {:?}",
                            e, client_key
                        ));
                    }
                },
            };

            let client_handlers;

            // Check if 'client_handlers' exists within 'kwargs'
            if let Some(handlers) = command.kwargs.get("client_handlers") {
                client_handlers = handlers;
            } else {
                return ProcessResult::Error(format!("restrictive_update_client_commands_ref give the followign error: The 'client_handlers' key does not exist within 'kwargs'."));
            }

            // } else {
            //     return ResultType::Error(format!("update_client_commands_ref command doesn't have kwargs in it!"));
            // }

            let mut client_name: String = client.get_client_name();

            {
                let mut controller = CLIENTS_SYNC_CONTROLLER.lock().await;
                let status = controller.update_client_sync_status(client_key, true);
                // TODO >>> Add a mechanism to set all the other clients state to sync = false
            }

            return ProcessResult::Empty;
        }
        "add_client" => {
            // > edit client
            // {'response_mode':'InternalManagement', 'activation_function':'add_client', 'kwargs':response, 'response_activation_function':'function_name'}
            // 'kwargs':{'new_client':clientpattern}

            // if !command.kwargs.contains_key("client_key") {
            //     logger.warn("Error! Callback response kwargs don't have client_key kwarg!".to_string());
            //     return ProcessResult::Error(format!("Error! Callback response kwargs don't have client_key kwarg!"));
            // }

            // if !command.kwargs.contains_key("new_client") {
            //     logger.warn("Error! Callback response kwargs don't have new_client kwarg!".to_string());
            //     return ProcessResult::Error(format!("Error! Callback response kwargs don't have new_client kwarg!"));
            // }

            // let client_key = command.kwargs.get("client_key").unwrap().as_str().unwrap();

            // from("client_name":"str", "client_key":"str", "client_type":"str", "permission_group":"str", "is_super_user":"bool", "max_sub_channels":"int", "owned_sub_channels_keys":"list")

            let new_client = match cast_new_client(&command.kwargs) {
                Ok(c) => c,
                Err(e) => return e, // TODO >>> Fix this error case
            };

            match new_client
                .save_into_db()
                .await
                .map_err(|e| ProcessResult::from(e))
            {
                Ok(_) => {}
                Err(e) => return e,
            }; //> It Already create the new client

            logger
                .debug("New client saved into the database!".to_string())
                .await;

            // TODO >>> Make a verification if the client already exists or not before add it!
            // let mut resp_kwargs: HashMap<String, Value> = HashMap::new();
            // resp_kwargs.insert("client_key".to_string(), Value::String(client_key.to_string()));

            //> Define the default acft
            let mut acft: String = "add_client_handler".to_string();

            if let Some(command_actf) = command.response_actf.clone() {
                acft = command_actf.to_string()
            }

            let new_command_instructions: CommandInstructions = CommandInstructions::new(
                CommandMode::Response,
                CommandType::ExternalFunction,
                CommandTarget::Origin,
                CommandStatus::Success,
                CommandOrigin::Host,
                acft,
                HashMap::new(),
                format!("Successfully add a client: {}!", new_client.client_key).to_string(),
                command.response_type,
                command.response_target,
                command.response_actf,
                command.collect_response,
            );

            logger
                .info(format!(
                    "Successfully add a client: {}!",
                    new_client.client_key
                ))
                .await;

            return ProcessResult::CommandInstructions(new_command_instructions);
        }
        "update_client" => {
            // > update client
            // {'response_mode':'InternalManagement', 'activation_function':'update_client', 'kwargs':response, 'response_activation_function':'function_name'}
            // 'kwargs':{'actual_client_key':String, 'updated_client':client} // Client have to have the same client key
            // 'client': {"client_name":str, "client_key":str, "client_type":str, "permission_group":str, "is_super_user":bool, "max_sub_channels":int, "owned_sub_channels_keys":list}

            logger
                .debug("Receive a update client inner command!".to_string())
                .await;

            if !command.kwargs.contains_key("actual_client_key") {
                logger
                    .warn(
                        "Error! Callback response kwargs don't have actual_client_key kwarg!"
                            .to_string(),
                    )
                    .await;
                return ProcessResult::Error(format!(
                    "Error! Callback response kwargs don't have actual_client_key kwarg!"
                ));
            }

            if !command.kwargs.contains_key("updated_client") {
                logger
                    .warn(
                        "ERROR, Error! Callback response kwargs don't have update_client kwarg!"
                            .to_string(),
                    )
                    .await;
                return ProcessResult::Error(format!(
                    "Error! Callback response kwargs don't have update_client kwarg!"
                ));
            }

            let actual_client_key = &command
                .kwargs
                .get("actual_client_key")
                .unwrap()
                .as_str()
                .unwrap()
                .clone();

            // from("client_name":"str", "client_key":"str", "client_type":"str", "permission_group":"str", "is_super_user":"bool", "max_sub_channels":"int", "owned_sub_channels_keys":"list")

            // TODO >>> Update this to use the new kwargs structure where the new client is wrapped in updated_client
            //> The idea was to send the content of the "updated_client" to the cast_new_client, cast the updated client
            //> and use the current client key to get the current client and change it to the new client casted

            // This function now accepts a serde_json::Value that is expected to be a String containing JSON
            fn convert(value: &Value) -> Result<HashMap<String, Value>, ProcessResult> {
                if let Value::String(ref json_string) = value {
                    // Parse the JSON string to a serde_json::Value
                    match serde_json::from_str::<Value>(&json_string) {
                        Ok(parsed_json_value) => {
                            // Convert the serde_json::Value to a HashMap<String, Value>
                            serde_json::from_value::<HashMap<String, Value>>(parsed_json_value)
                                .map_err(|e| {
                                    ProcessResult::Error(format!(
                                        "Failed to parse JSON to HashMap: {}",
                                        e
                                    ))
                                })
                        }
                        Err(e) => Err(ProcessResult::Error(format!(
                            "Failed to parse string to JSON: {}",
                            e
                        ))),
                    }
                } else {
                    Err(ProcessResult::Error("Expected a JSON string".to_string()))
                }
            }

            println!("command instructions kwargs: {:?}", &command.kwargs);

            let new_client = match command.kwargs.get("updated_client") {
                Some(map) => {
                    let converted_map = match convert(map) {
                        Ok(m) => m,
                        Err(e) => return e,
                    };
                    match cast_new_client(&converted_map) {
                        Ok(c) => c,
                        Err(e) => return e,
                    }
                }
                None => {
                    logger
                        .warn(format!(
                            "Error! Kwargs doesn't have the `updated_client` kwargs"
                        ))
                        .await;
                    return ProcessResult::Error(format!(
                        "Error! Kwargs doesn't have the `updated_client` kwargs"
                    ));
                }
            };

            let old_client = match Client::get_by_key(&actual_client_key.to_string()).await {
                Ok(old_c) => old_c,
                Err(e) => {
                    return ProcessResult::Error(format!(
                        "Client Error trying to get client by key: {:?}",
                        e
                    ));
                }
            };

            // TODO >>> Maybe implement a fast result-ype to client if needed

            match old_client.update_to(&new_client).await {
                //> It already saves into the database
                Ok(_) => {
                    let mut resp_kwargs: HashMap<String, Value> = HashMap::new();

                    resp_kwargs.insert(
                        "actual_client_key".to_string(),
                        Value::String(actual_client_key.to_string()),
                    ); // TODO >>> See if this actual client key is correct

                    //> Define the default acft
                    let mut acft: String = "update_client_handler".to_string();

                    if let Some(command_actf) = command.response_actf.clone() {
                        acft = command_actf.to_string()
                    }

                    let new_command_instructions: CommandInstructions = CommandInstructions::new(
                        CommandMode::Response,
                        CommandType::ExternalFunction,
                        CommandTarget::Origin,
                        CommandStatus::Success,
                        CommandOrigin::Host,
                        acft,
                        resp_kwargs,
                        format!(
                            "Successfully executed the function: {} and remove client: {}!",
                            activation_key, old_client.client_key
                        )
                        .to_string(),
                        command.response_type,
                        command.response_target,
                        command.response_actf,
                        command.collect_response,
                    );

                    logger
                        .info(format!(
                            "Successfully executed the function: {} and remove client: {}!",
                            activation_key, old_client.client_key
                        ))
                        .await;

                    return ProcessResult::CommandInstructions(new_command_instructions);
                }

                Err(e) => match e {
                    ClientError::ClientDoesNotExist(e) => {
                        logger
                            .warn(format!(
                                "Error! Can't Update client because client {} Don't exist!",
                                e
                            ))
                            .await;
                        return ProcessResult::Error(format!(
                            "Error! Can't Update client because client {} Don't exist!",
                            e
                        ));
                    }
                    _ => {
                        logger
                            .warn(
                                "Error! Can Update client because a unexpected error!".to_string(),
                            )
                            .await;
                        return ProcessResult::Error(format!(
                            "Error! Can Update client because a unexpected error!"
                        ));
                    }
                },
            }

            // TODO >>> Implement a mechanism to send back the confirmation or a error message originated from the operation
            // else {
            //     logger.warn("Error! Callback response kwargs isn't a Map!".to_string());
            //     return create_error_response_and_return!("Error! Callback response kwargs isn't a Map!", converted_m, to_send);
            // }
        }
        "remove_client" => {
            // > remove client
            // {'response_mode':'InternalManagement', 'activation_function':'remove_client', 'kwargs':response, 'response_activation_function':'function_name'}
            // 'kwargs':{'client_key':String}

            if !command.kwargs.contains_key("client_key") {
                return ProcessResult::Error(format!(
                    "Error! Callback response kwargs don't have client_key kwarg!"
                ));
            }

            let client_key: String = command
                .kwargs
                .get("client_key")
                .unwrap()
                .as_str()
                .map(|s| s.to_string())
                .unwrap();

            let client = match Client::get_by_key(&client_key.to_string()).await {
                Ok(old_c) => old_c,
                Err(e) => {
                    return ProcessResult::Error(format!(
                        "Client Error trying to get client by key: {:?}",
                        e
                    ));
                }
            };

            let result = client.delete().await;

            match result {
                Err(e) => match e {
                    ClientError::ClientDoesNotExist(e) => {
                        logger
                            .warn(format!(
                                "Error! Can't Remove client because client {} Don't exist!",
                                e
                            ))
                            .await;
                        return ProcessResult::Error(format!(
                            "Error! Can't Remove client because client {} Don't exist!",
                            e
                        ));
                    }
                    _ => {
                        logger
                            .warn(
                                "Error! Can Remove client because a unexpected error!".to_string(),
                            )
                            .await;
                        return ProcessResult::Error(format!(
                            "Error! Can Remove client because a unexpected error!"
                        ));
                    }
                },
                Ok(_) => {
                    // let mut resp_kwargs: HashMap<String, Value> = HashMap::new();
                    // resp_kwargs.insert("actual_client_key".to_string(), Value::String(client_key.to_string())); // TODO >>> See if this actual client key is correct

                    //> Define the default acft
                    let mut acft: String = "remove_client_handler".to_string();

                    if let Some(command_actf) = command.response_actf.clone() {
                        acft = command_actf.to_string()
                    }

                    let new_command_instructions: CommandInstructions = CommandInstructions::new(
                        CommandMode::Response,
                        CommandType::ExternalFunction,
                        CommandTarget::Origin,
                        CommandStatus::Success,
                        CommandOrigin::Host,
                        acft,
                        HashMap::new(),
                        format!(
                            "Successfully executed the function: {} and remove client: {}!",
                            activation_key, client_key
                        )
                        .to_string(),
                        command.response_type,
                        command.response_target,
                        command.response_actf,
                        command.collect_response,
                    );

                    logger
                        .info(format!(
                            "Successfully executed the function: {} and remove client: {}!",
                            activation_key, client_key
                        ))
                        .await;

                    return ProcessResult::CommandInstructions(new_command_instructions);
                }
            }
            // else {
            //     logger.warn("Error! Callback response kwargs isn't a Map!".to_string());
            //     return create_error_response_and_return!("Error! Callback response kwargs isn't a Map!", converted_m, to_send);
            // }

            // TODO >>> Implement a mechanism to send back the confirmation or a error message originated from the operation
        }
        _ => {
            return ProcessResult::Error(format!("unknown direct function"));
        }
    }
}
