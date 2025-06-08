use crate::common::structs::available_commands::CommandPatterns;
use crate::common::structs::results_structs::ResultType;
use crate::common::types::BufferError;

use crate::socket_client::states_manager::manager::{ClientState, StateManagerError};
use crate::{NodeStatus, CLIENT_NODE_CONFIGS, CLIENT_STATE_MANAGER};

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
            let log_level = CLIENT_LOG_LEVEL.lock().await.clone();
            client_log_level = log_level.clone();
        }
        Logger::new(client_log_level, $section_name).await
    }};
}

impl From<BufferError> for ProcessError {
    fn from(e: BufferError) -> ProcessError {
        match e {
            BufferError::UnexpectedError(e) => ProcessError::BufferError(e),
        }
    }
}

pub async fn handle_direct_function(c: &CommandInstructions, client_key: &String, command_id: u32) -> Result<ProcessResult, ProcessError> {
    let logger = acquire_logger!("Transposer - Process");
    logger.info(format!("Initializing Direct Function processing!")).await;

    // TODO >>> Change this for a match

    // -> SESSION SYNCHRONIZATION:
    match c.actf.as_str() {
        "update_available_host_commands" => {
            logger.info(format!("Receive Host Allowed Commands")).await;

            // Clone the object to get a HashMap<String, Value>
            let response_map: HashMap<String, Value> = c.kwargs.clone();

            // TODO >>> Maybe create a mechanism to validate the new_patterns received, maybe using regex, idk...

            logger.debug("[CLIENT][GLOBAL][Try Lock] - HOST_ALLOWED_COMMANDS".to_string()).await;
            let mut filtered_commands_map = HashMap::new();
            logger.debug("Try to update the schedule with the new client state".to_string()).await;

            {
                let mut host_allowed_commands = HOST_ALLOWED_COMMANDS.lock().await;
                logger.debug("[CLIENT][GLOBAL][Lock] - HOST_ALLOWED_COMMANDS".to_string()).await;

                match host_allowed_commands.update_from_value_map(response_map) {
                    Ok(_) => {},
                    Err(e) => {
                        // TODO >>> Better threat this error case
                    },
                }

                let state_manager: Option<ClientState>;

                // -> CLIENT STATE NETWORK MAP LOADING
                let mut client_state = {
                    match ClientState::load_from_storage().await {
                        Ok(c) => c,
                        Err(e) => {
                            match e {
                                StateManagerError::NotFullyInitialized => {
                                    logger.exception(format!("Error trying to update client states in direct_functions, not fully initialized!")).await;
                                },
                                StateManagerError::CantgetStateFromDb(e) => {
                                    logger.exception(format!("Error trying to load client state from db, the error was: {:?}", e)).await;
                                },
                                StateManagerError::ErrorWhileSavingClientState(e) => {
                                    logger.exception(format!("Error trying to save client state in the database, the error was: {:?}", e)).await;
                                },
                            };
                            CLIENT_STATE_MANAGER.lock().clone()
                        },
                    }
                };

                client_state.network_map = Some(host_allowed_commands.clone());
                client_state.is_ready = Some(true);
                client_state.is_connected = Some(true);
                client_state.is_sync = Some(true);

                match &client_state.update_schedule_with_this().await {
                    Ok(_) => {},
                    Err(e) => match &client_state.save_in_storage().await {
                        Ok(_) => {},
                        Err(e) => match e {
                            StateManagerError::NotFullyInitialized => {
                                logger.exception(format!("Error trying to update client states in direct_functions, not fully initialized!")).await;
                            },
                            StateManagerError::CantgetStateFromDb(e) => {
                                logger.exception(format!("Error trying to load client state from db, the error was: {:?}", e)).await;
                            },
                            StateManagerError::ErrorWhileSavingClientState(e) => {
                                logger.exception(format!("Error trying to save client state in the database, the error was: {:?}", e)).await;
                            },
                        },
                    },
                };

                // TODO >>> Improve the error handling in the direct functions, they should better treat the error, for example:
                // > We should return an error back to the client or do something about the connection, it can't just continue to give the error, eveen that it will close the connect for not sync correctly after a while!
                // > We should do something else toguether with the loggin, not only log the error, one example would be count the error attempts and then disconnect the client, better treat this to avoid unecessary use of ressources

                logger.debug("[CLIENT][GLOBAL][Release] - HOST_ALLOWED_COMMANDS".to_string()).await;
                let actual_patterns: Value;
                logger.debug("[CLIENT][GLOBAL][Try Lock] - CLIENT_NODE_CONFIGS".to_string()).await;

                {
                    let mut command_patterns = CLIENT_NODE_CONFIGS.lock().await;
                    logger.debug("[CLIENT][GLOBAL][Lock] - CLIENT_NODE_CONFIGS".to_string()).await;
                    logger.info(format!("Lock In Host Command Patterns!")).await;
                    command_patterns.change_node_status(NodeStatus::Online);
                    command_patterns.update_known_network(host_allowed_commands.get_all_nodes_except_node_with_key(&"".to_string()).clone());
                    actual_patterns = command_patterns.to_value();
                }

                logger.debug("[CLIENT][GLOBAL][Release] - CLIENT_NODE_CONFIGS".to_string()).await;
                logger.info(format!("Successfully actualize the host available commands!")).await;

                // TODO >>> Change this to use NetworkMap instead of commands
                filtered_commands_map.insert("client_handlers".to_string(), actual_patterns);
            }

            println!("Successfully update the schedule with the new client state");
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

            logger.info("Finish building".to_string()).await;
            let parity_id: String = enhanced_buffer::buffer_up_manager::buffer_up_gen_valid_parity_id(client_key.clone()).await.map_err(ProcessError::from)?;

            let up_command: UpCommand = UpCommand::new(client_key, &parity_id, 11u8, &to_string(&new_command_instructions).unwrap());
            enhanced_buffer::buffer_up_manager::buffer_up_schedule(up_command).await.map_err(|e| ProcessError::from(e))?;

            //> TURN CLIENT SYNC STATUS TO TRUE
            CLIENT_IS_SYNC.store(true, Ordering::SeqCst);

            enhanced_buffer::buffer_down_manager::buffer_down_remove_schedule_by_id(command_id.clone()).await.map_err(|e| ProcessError::from(e))?;
            return Ok(ProcessResult::Empty);
        },
        "get_socket_client_available_handlers" => {
            // -> DINAMIC RESPONSE IMPLEMENTED:

            logger.info(format!("Receive Available Handlers Request")).await;

            // Lock the CLIENT_NODE_CONFIGS and insert the new map

            let actual_patterns: Value;
            logger.debug("[CLIENT][GLOBAL][Try Lock] - CLIENT_NODE_CONFIGS".to_string()).await;

            {
                let command_patterns = CLIENT_NODE_CONFIGS.lock().await;
                logger.debug("[CLIENT][GLOBAL][Lock] - CLIENT_NODE_CONFIGS".to_string()).await;
                actual_patterns = command_patterns.to_value();
            }

            logger.debug("[CLIENT][GLOBAL][Release] - CLIENT_NODE_CONFIGS".to_string()).await;
            logger.info(format!("Successfully actualize the host available commands!")).await;

            enhanced_buffer::buffer_down_manager::buffer_down_remove_schedule_by_id(command_id.clone()).await.map_err(|e| ProcessError::from(e))?;
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

            let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(1).enable_all().build().expect("Failed to create Tokio runtime");
            let parity_id: String = rt.block_on(async { enhanced_buffer::buffer_up_manager::buffer_up_gen_valid_parity_id(client_key.clone()).await.map_err(ProcessError::from) })?;

            let up_command: UpCommand = UpCommand::new(client_key, &parity_id, 11u8, &to_string(&new_command_instructions).unwrap());

            enhanced_buffer::buffer_up_manager::buffer_up_schedule(up_command).await.map_err(|e| ProcessError::from(e))?;
            enhanced_buffer::buffer_down_manager::buffer_down_remove_schedule_by_id(command_id.clone()).await.map_err(|e| ProcessError::from(e))?;
            enhanced_buffer::buffer_down_manager::buffer_down_remove_schedule_by_id(command_id.clone()).await.map_err(|e| ProcessError::from(e))?;

            return Ok(ProcessResult::Empty);
        },

        // -> VITAL NETWORK COMPONENTS
        "update_client_network_rechable" => {
            return Ok(ProcessResult::Empty); // TODO >>> Implement the mechanism to allow update the Client Notion about the remote handlers
        },

        // -> GENERAL OUT OF SCOPE CASES:
        _ => {
            return Err(ProcessError::CommandNotRegistered(c.actf.clone()));
        },
    }
}
