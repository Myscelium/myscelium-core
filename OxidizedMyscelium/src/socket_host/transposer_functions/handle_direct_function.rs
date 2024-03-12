use crate::chrono::TimeZone;
use crate::common::client_manager::manager::get_all_clients;
use crate::common::client_manager::manager::{check_if_client_key_exists, Client, ClientError};
use crate::common::enhanced_buffer;
use crate::common::enhanced_buffer::utilities::{Command, CommandInstructions, CommandMode, CommandOrigin, CommandStatus, CommandTarget, CommandType, ResponseTarget, ResponseType};
use crate::common::functions::converters::{convert_json_map_to_hash_map, convert_value_map_to_resulttype_map, ConversionError};
use crate::common::structs::available_commands::NodeStatus;
use crate::common::structs::available_commands::{CommandPatterns, Node};
use crate::common::structs::results_structs::ResultType;
use crate::handle_manager_client_error;
use crate::socket_client::transposer::ProcessError;
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
            let log_level = HOST_LOG_LEVEL.lock();
            host_log_level = log_level.clone()
        }
        Logger::new(host_log_level, $section_name)
    }};
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ProcessResult {
    Empty,
    List(Vec<ProcessResult>),
    Error(String),
    CommandInstructions(CommandInstructions),
}

pub fn handle_direct_function(client_key: &String, activation_key: &String, command: CommandInstructions, command_id: Option<u32>) -> ProcessResult {
    let logger = acquire_logger!("Transposer - Process - Handle Direct Functions");

    logger.info(format!("Initializing processing!"));

    logger.debug(format!("function received in handle direct function: {}", activation_key));

    // -> ----------------------------------------------------------------------------------------------------------------------------------
    // -> SYNCRONIZATION MECHANISM

    match activation_key.as_str() {
        "get_registered_commands" => {
            logger.info(format!("Receive get_registered_commands in host!"));

            // Lock the HOST_COMMAND_PATTERNS and insert the new map

            let actual_patterns;

            {
                actual_patterns = HOST_COMMAND_PATTERNS.lock().clone();
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
            filtered_commands.insert("network_nodes".to_string(), serde_json::to_value(nodes).unwrap());

            logger.info(format!("Successfully actualize the host available commands!"));

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
            );

            return ProcessResult::CommandInstructions(new_command_instructions);
        },
        "update_client_commands_ref" => {
            logger.info(format!("Receive update_client_commands_ref in host!"));

            // -> get the client by the client key
            let client = match Client::get_by_key(client_key) {
                Ok(c) => c,
                Err(e) => match e {
                    ClientError::ClientDoesNotExist(_) => {
                        return ProcessResult::Error(format!("Unknow client_key: {:?}", client_key));
                    },
                    _ => {
                        return ProcessResult::Error(format!("Get a error {:?}, obtaining client: {:?}", e, client_key));
                    },
                },
            };

            let client_handlers;

            // Check if 'client_handlers' exists within 'kwargs'
            if let Some(handlers) = command.kwargs.get("client_handlers") {
                client_handlers = handlers;
            } else {
                return ProcessResult::Error(format!("update_client_commands_ref give the followign error: The 'client_handlers' key does not exist within 'kwargs'."));
            }

            // } else {
            //     return ResultType::Error(format!("update_client_commands_ref command doesn't have kwargs in it!"));
            // }

            let mut client_name: String = client.get_client_name();

            {
                let mut actual_patterns = HOST_COMMAND_PATTERNS.lock();

                let mut client_node = match Node::from_value(client_handlers.clone()) {
                    Ok(n) => n,
                    Err(e) => {
                        return ProcessResult::Error(format!("Error creating node, the error was: {:?}", e));
                    },
                };

                client_node.change_node_status(NodeStatus::Online);
                actual_patterns.add_or_update_if_exists(client_node);
            }

            {
                let mut controller = CLIENTS_SYNC_CONTROLLER.lock();
                let status = controller.update_client_sync_status(client_key, true);
                // TODO >>> Add a mechanism to set all the other clients state to sync = false
            }

            // -> Try to get the clients registred in the database
            let mut clients = match get_all_clients() {
                Ok(c) => c,
                Err(e) => match e {
                    _ => {
                        // TODO >>> Create a better error handling for this, there is no need to return this to any client

                        let new_command_instructions = CommandInstructions::new(
                            CommandMode::Function,
                            CommandType::DirectFunction,
                            CommandTarget::Origin,
                            CommandStatus::Failure,
                            CommandOrigin::Host,
                            "update_available_host_commands".to_string(),
                            HashMap::new(),
                            "unexpect error getting clients to redirect the update commands".to_string(),
                            Some(ResponseType::DirectFunction),
                            Some(ResponseTarget::Host),
                            None, // Not required in this case
                        );

                        return ProcessResult::CommandInstructions(new_command_instructions);
                    },
                },
            };

            // -> Filter the actual client from the list cause it alwready was handled
            for (index, client) in clients.iter().enumerate() {
                if client.client_key == client_key.clone() {
                    clients.remove(index);
                    break;
                }
            }

            let mut responses: Vec<ProcessResult> = Vec::new();

            logger.info(format!("Receive client: {} handlers, retransmitting to: {:?}", client_key, clients).to_string());

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
            );

            responses.push(ProcessResult::CommandInstructions(new_command_instructions));

            // -> Send the updated info for all the clients
            for client in clients {
                //> See if client has some alive signal in the last 30s:

                // Split into seconds and nanoseconds
                let seconds = client.last_contact.trunc() as i64;
                let nanoseconds = (client.last_contact.fract() * 1e9).round() as u32; // Or *1_000_000_000.0

                // Convert to DateTime<Utc>
                let last_contact = Utc.timestamp_opt(seconds, nanoseconds).unwrap();

                let current_time = Utc::now();
                if current_time - last_contact > Duration::seconds(30) {
                    continue;
                }

                //> Redirect new commands to client if changed:

                let mut nodes: Vec<Node> = Vec::new();

                // TODO >>> Add a mechanism to see what handlers the client will ahve permission to activate
                //* Any mechanism that will see the client permissions to each command may be placed here

                // > Schedule a redirect to the other clients
                let client_key_to_redirect: String = client.client_key.clone();

                {
                    let actual_patterns = HOST_COMMAND_PATTERNS.lock();
                    // TODO >>> Change to get all nodes except for node x
                    nodes = actual_patterns.get_all_nodes_except_node_with_key(&client_key_to_redirect);
                }

                let mut filtered_commands: HashMap<String, Value> = HashMap::new();
                filtered_commands.insert("network_nodes".to_string(), serde_json::to_value(nodes).unwrap());
                let new_command_instructions = CommandInstructions::new(
                    CommandMode::Response,
                    CommandType::DirectFunction,
                    CommandTarget::ClientKey(client_key_to_redirect),
                    CommandStatus::Success,
                    CommandOrigin::Host,
                    "update_available_host_commands".to_string(),
                    filtered_commands,
                    "".to_string(),
                    Some(ResponseType::DirectFunction),
                    Some(ResponseTarget::Host),
                    None, // Not required in this case
                );

                responses.push(ProcessResult::CommandInstructions(new_command_instructions));

                return ProcessResult::List(responses);
            }

            return ProcessResult::List(responses);
        },
        "restrictive_update_client_commands_ref" => {
            logger.info(format!("Receive restrictive_update_client_commands_ref in host!"));

            // -> get the client by the client key
            let client = match Client::get_by_key(client_key) {
                Ok(c) => c,
                Err(e) => match e {
                    ClientError::ClientDoesNotExist(_) => {
                        return ProcessResult::Error(format!("Unknow client_key: {:?}", client_key));
                    },
                    _ => {
                        return ProcessResult::Error(format!("Get a error {:?}, obtaining client: {:?}", e, client_key));
                    },
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
                let mut controller = CLIENTS_SYNC_CONTROLLER.lock();
                let status = controller.update_client_sync_status(client_key, true);
                // TODO >>> Add a mechanism to set all the other clients state to sync = false
            }

            return ProcessResult::Empty;
        },
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

            new_client.save_into_db(); //> It Already create the new client

            logger.debug("New client saved into the database!".to_string());

            // TODO >>> Make a verification if the client already exists or not before add it!
            // let mut resp_kwargs: HashMap<String, Value> = HashMap::new();
            // resp_kwargs.insert("client_key".to_string(), Value::String(client_key.to_string()));

            //> Define the default acft
            let mut acft: String = "add_client_handler".to_string();

            if let Some(actf) = command.response_target.clone() {
                acft = acft
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
            );

            logger.info(format!("Successfully add a client: {}!", new_client.client_key));

            return ProcessResult::CommandInstructions(new_command_instructions);
        },
        "update_client" => {
            // > update client
            // {'response_mode':'InternalManagement', 'activation_function':'update_client', 'kwargs':response, 'response_activation_function':'function_name'}
            // 'kwargs':{'actual_client_key':String, 'updated_client':client} // Client have to have the same client key
            // 'client': {"client_name":str, "client_key":str, "client_type":str, "permission_group":str, "is_super_user":bool, "max_sub_channels":int, "owned_sub_channels_keys":list}

            logger.debug("Receive a update client inner command!".to_string());

            if !command.kwargs.contains_key("actual_client_key") {
                logger.warn("Error! Callback response kwargs don't have actual_client_key kwarg!".to_string());
                return ProcessResult::Error(format!("Error! Callback response kwargs don't have actual_client_key kwarg!"));
            }

            if !command.kwargs.contains_key("updated_client") {
                logger.warn("ERROR, Error! Callback response kwargs don't have update_client kwarg!".to_string());
                return ProcessResult::Error(format!("Error! Callback response kwargs don't have update_client kwarg!"));
            }

            let actual_client_key = &command.kwargs.get("actual_client_key").unwrap().as_str().unwrap().clone();

            // from("client_name":"str", "client_key":"str", "client_type":"str", "permission_group":"str", "is_super_user":"bool", "max_sub_channels":"int", "owned_sub_channels_keys":"list")

            // TODO >>> Update this to use the new kwargs structure where the new client is wrapped in updated_client
            //> The idea was to send the content of the "updated_client" to the cast_new_client, cast the updated client
            //> and use the current clietn key to get the current client and change it to the new client casted

            // fn convert(map: &serde_json::Map<String, Value>) -> HashMap<String, Value> {
            //     map.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
            // }

            // This function now accepts a serde_json::Value that is expected to be a String containing JSON
            fn convert(value: &Value) -> Result<HashMap<String, Value>, ProcessResult> {
                if let Value::String(ref json_string) = value {
                    // Parse the JSON string to a serde_json::Value
                    match serde_json::from_str::<Value>(&json_string) {
                        Ok(parsed_json_value) => {
                            // Convert the serde_json::Value to a HashMap<String, Value>
                            serde_json::from_value::<HashMap<String, Value>>(parsed_json_value).map_err(|e| ProcessResult::Error(format!("Failed to parse JSON to HashMap: {}", e)))
                        },
                        Err(e) => Err(ProcessResult::Error(format!("Failed to parse string to JSON: {}", e))),
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
                },
                None => {
                    logger.warn(format!("Error! Kwargs doesn't have the `updated_client` kwargs"));
                    return ProcessResult::Error(format!("Error! Kwargs doesn't have the `updated_client` kwargs"));
                },
            };

            let old_client = handle_manager_client_error!(Client::get_by_key(&actual_client_key.to_string()));

            // TODO >>> Maybe implement a fast result-ype to client if needed

            match old_client.update_to(&new_client) {
                //> It already saves into the database
                Ok(_) => {
                    let mut resp_kwargs: HashMap<String, Value> = HashMap::new();

                    resp_kwargs.insert("actual_client_key".to_string(), Value::String(actual_client_key.to_string())); // TODO >>> See if this actual client key is correct

                    //> Define the default acft
                    let mut acft: String = "update_client_handler".to_string();

                    if let Some(actf) = command.response_target.clone() {
                        acft = acft
                    }

                    let new_command_instructions: CommandInstructions = CommandInstructions::new(
                        CommandMode::Response,
                        CommandType::ExternalFunction,
                        CommandTarget::Origin,
                        CommandStatus::Success,
                        CommandOrigin::Host,
                        acft,
                        resp_kwargs,
                        format!("Successfully executed the function: {} and remove client: {}!", activation_key, old_client.client_key).to_string(),
                        command.response_type,
                        command.response_target,
                        command.response_actf,
                    );

                    logger.info(format!("Successfully executed the function: {} and remove client: {}!", activation_key, old_client.client_key));

                    return ProcessResult::CommandInstructions(new_command_instructions);
                },

                Err(e) => match e {
                    ClientError::ClientDoesNotExist(e) => {
                        logger.warn(format!("Error! Can't Update client because client {} Don't exist!", e));
                        return ProcessResult::Error(format!("Error! Can't Update client because client {} Don't exist!", e));
                    },
                    _ => {
                        logger.warn("Error! Can Update client because a unexpected error!".to_string());
                        return ProcessResult::Error(format!("Error! Can Update client because a unexpected error!"));
                    },
                },
            }

            // TODO >>> Implement a mechanism to send back the confirmation or a error message originated from the operation
            // else {
            //     logger.warn("Error! Callback response kwargs isn't a Map!".to_string());
            //     return create_error_response_and_return!("Error! Callback response kwargs isn't a Map!", converted_m, to_send);
            // }
        },
        "remove_client" => {
            // > remove client
            // {'response_mode':'InternalManagement', 'activation_function':'remove_client', 'kwargs':response, 'response_activation_function':'function_name'}
            // 'kwargs':{'client_key':String}

            if !command.kwargs.contains_key("client_key") {
                return ProcessResult::Error(format!("Error! Callback response kwargs don't have client_key kwarg!"));
            }

            let client_key: String = command.kwargs.get("client_key").unwrap().as_str().map(|s| s.to_string()).unwrap();

            let client = handle_manager_client_error!(Client::get_by_key(&client_key));

            let result = client.delete();

            match result {
                Err(e) => match e {
                    ClientError::ClientDoesNotExist(e) => {
                        logger.warn(format!("Error! Can't Remove client because client {} Don't exist!", e));
                        return ProcessResult::Error(format!("Error! Can't Remove client because client {} Don't exist!", e));
                    },
                    _ => {
                        logger.warn("Error! Can Remove client because a unexpected error!".to_string());
                        return ProcessResult::Error(format!("Error! Can Remove client because a unexpected error!"));
                    },
                },
                Ok(_) => {
                    // let mut resp_kwargs: HashMap<String, Value> = HashMap::new();
                    // resp_kwargs.insert("actual_client_key".to_string(), Value::String(client_key.to_string())); // TODO >>> See if this actual client key is correct

                    //> Define the default acft
                    let mut acft: String = "remove_client_handler".to_string();

                    if let Some(actf) = command.response_target.clone() {
                        acft = acft
                    }

                    let new_command_instructions: CommandInstructions = CommandInstructions::new(
                        CommandMode::Response,
                        CommandType::ExternalFunction,
                        CommandTarget::Origin,
                        CommandStatus::Success,
                        CommandOrigin::Host,
                        acft,
                        HashMap::new(),
                        format!("Successfully executed the function: {} and remove client: {}!", activation_key, client_key).to_string(),
                        command.response_type,
                        command.response_target,
                        command.response_actf,
                    );

                    logger.info(format!("Successfully executed the function: {} and remove client: {}!", activation_key, client_key));

                    return ProcessResult::CommandInstructions(new_command_instructions);
                },
            }
            // else {
            //     logger.warn("Error! Callback response kwargs isn't a Map!".to_string());
            //     return create_error_response_and_return!("Error! Callback response kwargs isn't a Map!", converted_m, to_send);
            // }

            // TODO >>> Implement a mechanism to send back the confirmation or a error message originated from the operation
        },
        _ => {
            return ProcessResult::Error(format!("unknow direct function"));
        },
    }
}
