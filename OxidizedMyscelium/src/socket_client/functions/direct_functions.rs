use crate::common::structs::available_commands::CommandPatterns;
use crate::common::structs::results_structs::ResultType;

use crate::socket_client::states_manager::manager::{ClientState, StateManagerError};
use crate::{CLIENT_NODE_CONFIGS, CLIENT_STATE_MANAGER};

use crate::socket_client::transposer::ProcessError;
use crate::socket_host::transposer_functions::handle_direct_function::ProcessResult;
use serde_json::{to_string, Value};
use std::collections::HashMap;

use crate::socket_client::client_logger::log_handler::Logger;
use crate::{CLIENT_IS_SYNC, CLIENT_LOG_LEVEL};

use crate::HOST_ALLOWED_COMMANDS;

use crate::common::enhanced_buffer;
use crate::common::enhanced_buffer::utilities::{Command, CommandInstructions, CommandMode, CommandOrigin, CommandStatus, CommandTarget, CommandType, ResponseTarget, ResponseType};
use crate::common::functions::converters::convert_to_value_map;
use crate::common::functions::converters::convert_value_map_to_resulttype_map;
use crate::common::functions::converters::ConversionError;
use crate::socket_client::functions::direct_functions::enhanced_buffer::buffer_up_manager::UpCommand;

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

macro_rules! acquire_logger {
    ($section_name:expr) => {{
        let client_log_level;
        {
            let log_level = CLIENT_LOG_LEVEL.lock().clone();
            client_log_level = log_level.clone();
        }
        Logger::new(client_log_level, $section_name)
    }};
}

pub fn handle_direct_function(c: &CommandInstructions, client_key: &String, command_id: u32) -> Result<ProcessResult, ProcessError> {
    let logger = acquire_logger!("Transposer - Process");

    logger.info(format!("Initializing Direct Function processing!"));

    // TODO >>> Change this for a mathc

    // -> SESSION SYNCHRONIZATION

    match c.actf.as_str() {
        "update_available_host_commands" => {
            logger.info(format!("Receive Host Allowed Commands"));

            // Clone the object to get a HashMap<String, Value>
            let response_map: HashMap<String, Value> = c.kwargs.clone();

            // TODO >>> Maybe create a mechanism to validate the new_patterns received, maybe using regex, idk...

            println!("[CLIENT][GLOBAL][Try Lock] - HOST_ALLOWED_COMMANDS");

            {
                let mut host_allowed_commands = HOST_ALLOWED_COMMANDS.lock();
                println!("[CLIENT][GLOBAL][Lock] - HOST_ALLOWED_COMMANDS");
                match host_allowed_commands.update_from_value_map(response_map) {
                    Ok(_) => {},
                    Err(e) => {
                        // TODO >>> Better threat this error case
                    },
                }

                // -> CLIENT STATE NETWORK MAP LOADING
                let mut client_state = match ClientState::load_from_storage() {
                    Ok(c) => c,
                    Err(e) => {
                        match e {
                            StateManagerError::NotFullyInitialized => {
                                logger.exception(format!("Error trying to update client states in direct_functions, not fully initialized!"));
                            },
                            StateManagerError::CantgetStateFromDb(e) => {
                                logger.exception(format!("Error trying to load client state from db, the error was: {:?}", e));
                            },
                        };
                        CLIENT_STATE_MANAGER.lock().clone()
                    },
                };
                client_state.network_map = Some(host_allowed_commands.clone());
                client_state.is_ready = Some(true);
                client_state.is_connected = Some(true);
                client_state.is_sync = Some(true);
                match &client_state.update_schedule_with_this() {
                    Ok(_) => {},
                    Err(e) => match &client_state.save_in_storage() {
                        Ok(_) => {},
                        Err(e) => match e {
                            StateManagerError::NotFullyInitialized => {
                                logger.exception(format!("Error trying to update client states in direct_functions, not fully initialized!"));
                            },
                            StateManagerError::CantgetStateFromDb(e) => {
                                logger.exception(format!("Error trying to load client state from db, the error was: {:?}", e));
                            },
                        },
                    },
                };
            }

            println!("[CLIENT][GLOBAL][Release] - HOST_ALLOWED_COMMANDS");
            let actual_patterns: Value;
            println!("[CLIENT][GLOBAL][Try Lock] - CLIENT_NODE_CONFIGS");

            {
                let command_patterns = CLIENT_NODE_CONFIGS.lock();
                println!("[CLIENT][GLOBAL][Lock] - CLIENT_NODE_CONFIGS");
                logger.info(format!("Lock In Host Command Patterns!"));
                actual_patterns = command_patterns.to_value();
            }

            println!("[CLIENT][GLOBAL][Release] - CLIENT_NODE_CONFIGS");

            logger.info(format!("Successfully actualize the host available commands!"));

            // TODO >>> Change this to use NetworkMap instead of commands
            let mut filtered_commands_map = HashMap::new();
            filtered_commands_map.insert("client_handlers".to_string(), actual_patterns);

            //> VERIFY IF IS CLIENT FIRST SYNC
            if !CLIENT_IS_SYNC.load(Ordering::SeqCst) {
                // -> Only return this 'update_client_commands_ref' in case that is the first sync of the client

                // TODO >>> Maybe change this to return the command instead of schedule it manually to send to host
                let new_command_instructions = CommandInstructions::new(
                    CommandMode::Function,
                    CommandType::DirectFunction,
                    CommandTarget::Host,
                    CommandStatus::Success,
                    CommandOrigin::ClientKey(client_key.clone()),
                    "update_client_commands_ref".to_string(),
                    filtered_commands_map,
                    "".to_string(),
                    Some(ResponseType::DirectFunction),
                    Some(ResponseTarget::Origin),
                    None, // Not required in this case
                    true,
                );

                // > This need to be scheduled this way since this is a new command and need a new parity id, if return this will use the parity id received
                // TODO >>> A possible way to do this is by call the schedule instead of schedule by hand, maybe is a better option to avoid code repetition

                let parity_id = enhanced_buffer::buffer_up_manager::buffer_up_gen_valid_parity_id(client_key.clone());
                let up_command: UpCommand = UpCommand::new(client_key, &parity_id, 11u8, &to_string(&new_command_instructions).unwrap());
                enhanced_buffer::buffer_up_manager::buffer_up_schedule(up_command);
            } else {
                // -> Only return this 'restrictive_update_client_commands_ref' in case that isn't the first sync of the client to avoid network status loops

                // TODO >>> Maybe change this to return the command instead of schedule it manually to send to host
                let new_command_instructions = CommandInstructions::new(
                    CommandMode::Function,
                    CommandType::DirectFunction,
                    CommandTarget::Host,
                    CommandStatus::Success,
                    CommandOrigin::ClientKey(client_key.clone()),
                    "restrictive_update_client_commands_ref".to_string(),
                    filtered_commands_map,
                    "".to_string(),
                    Some(ResponseType::DirectFunction),
                    Some(ResponseTarget::Origin),
                    None, // Not required in this case
                    true,
                );

                let parity_id = enhanced_buffer::buffer_up_manager::buffer_up_gen_valid_parity_id(client_key.clone());
                let up_command: UpCommand = UpCommand::new(client_key, &parity_id, 11u8, &to_string(&new_command_instructions).unwrap());
                enhanced_buffer::buffer_up_manager::buffer_up_schedule(up_command);
            }

            //> TURN CLIENT SYNC STATUS TO TRUE
            CLIENT_IS_SYNC.store(true, Ordering::SeqCst);

            enhanced_buffer::buffer_down_manager::buffer_down_remove_schedule_by_id(command_id.clone());
            return Ok(ProcessResult::Empty);
        },
        "get_socket_client_available_handlers" => {
            // -> DINAMIC RESPONSE IMPLEMENTED:

            logger.info(format!("Receive Available Handlers Request"));

            // Lock the CLIENT_NODE_CONFIGS and insert the new map

            let actual_patterns: Value;

            println!("[CLIENT][GLOBAL][Try Lock] - CLIENT_NODE_CONFIGS");
            {
                let command_patterns = CLIENT_NODE_CONFIGS.lock();
                println!("[CLIENT][GLOBAL][Lock] - CLIENT_NODE_CONFIGS");
                actual_patterns = command_patterns.to_value();
            }
            println!("[CLIENT][GLOBAL][Release] - CLIENT_NODE_CONFIGS");

            logger.info(format!("Successfully actualize the host available commands!"));

            enhanced_buffer::buffer_down_manager::buffer_down_remove_schedule_by_id(command_id.clone());

            let mut filtered_commands_map: HashMap<String, Value> = HashMap::new();
            filtered_commands_map.insert("client_handlers".to_string(), actual_patterns);

            let new_command_instructions = CommandInstructions::new(
                CommandMode::Response,
                CommandType::DirectFunction,
                CommandTarget::Host,
                CommandStatus::Success,
                CommandOrigin::ClientKey(client_key.clone()),
                "update_client_commands_ref".to_string(),
                filtered_commands_map,
                "".to_string(),
                c.response_type.clone(),
                c.response_target.clone(),
                c.response_actf.clone(),
                true,
            );

            let parity_id = enhanced_buffer::buffer_up_manager::buffer_up_gen_valid_parity_id(client_key.clone());
            let up_command: UpCommand = UpCommand::new(client_key, &parity_id, 11u8, &to_string(&new_command_instructions).unwrap());

            enhanced_buffer::buffer_up_manager::buffer_up_schedule(up_command);
            enhanced_buffer::buffer_down_manager::buffer_down_remove_schedule_by_id(command_id.clone());
            enhanced_buffer::buffer_down_manager::buffer_down_remove_schedule_by_id(command_id.clone());

            return Ok(ProcessResult::Empty);
        },

        // -> VITAL NETWORK COMPONENTS
        "update_client_network_rechable" => {
            // TODO >>> Implement the mechanism to allow update the Client Notion about the remote handlers

            return Ok(ProcessResult::Empty);
        },

        // -> GENERAL OUT OF SCOPE CASES:
        _ => {
            return Err(ProcessError::CommandNotRegistered(c.actf.clone()));
        },
    }
}
