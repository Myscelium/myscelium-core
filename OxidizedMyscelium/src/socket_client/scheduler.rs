// SPDX-License-Identifier: MPL-2.0
// Copyright © 2021-2026 Cristian Camargo Filho

use crate::common::enhanced_buffer;
use crate::common::enhanced_buffer::buffer_down_manager::DownCommand;
use crate::common::enhanced_buffer::buffer_up_manager::UpCommand;
use crate::common::enhanced_buffer::utilities::{
    Command, CommandInstructions, CommandTarget, CommandType, ResponseTarget,
};
// use crate::common::functions::advanced_lockers::smart_lock;
use crate::common::types::{BufferError, SchedulingError};
use crate::socket_client::states_manager::manager::ClientState;

use lazy_static::lazy_static;

use serde_json::{from_str, Value};
use std::collections::HashMap;

use super::client_logger::log_handler::Logger;
use crate::{Client, CLIENT_LOG_LEVEL, CLIENT_STATE_MANAGER};

use parking_lot::Mutex;

use std::sync::Arc;

macro_rules! acquire_logger {
    ($section_name:expr) => {{
        let client_log_level;
        {
            client_log_level = CLIENT_LOG_LEVEL.lock().await.clone();
        }
        Logger::new(client_log_level, $section_name).await
    }};
}

/// Schedules a command for processing.
///
/// The function takes in a command and its priority, then schedules it for processing
/// by converting the command to a string, generating a unique parity ID, and adding it
/// to the up buffer manager's schedule.
///
/// # Arguments
/// - `command`: A map representing the command to be scheduled.
/// - `priority`: The priority level of the command. Commands with higher priority values
///               are processed before those with lower priority values.
pub async fn schedule(
    command_instructions: CommandInstructions,
    priority: u8,
) -> Result<String, SchedulingError> {
    let mut command_instructions: CommandInstructions = command_instructions;

    // -- Logging Setup:
    let logger: Logger = acquire_logger!("Core - Scheduler");
    logger.debug("Enter Scheduler".to_string()).await;
    logger
        .debug(format!("[CLIENT][GLOBAL][Try Lock] - CLIENT_ID"))
        .await;

    // -- Load ClientState from storage using `.await` directly
    let maybe_state: Option<ClientState> = ClientState::load_from_storage()
        .await
        .map(Some)
        .map_err(|_| SchedulingError::CantReadStates)?;
    let mut state_manager: ClientState = if let Some(state) = maybe_state {
        state
    } else {
        // Shouldn’t happen—`load_from_storage` returned Ok(None)? But we treat as error.
        return Err(SchedulingError::CantReadStates);
    };

    // if !state_manager.is_fully_initialized() {
    //    return Err(SchedulingError::ClientIsntFullyInitialized);
    //}
    // TODO >>> Make a universal function to verify this kind of conflict prevention

    // -- Check initialization flags
    if let Some(ready) = state_manager.is_ready {
        if !ready {
            return Err(SchedulingError::ClientIsntFullyInitialized(
                command_instructions.origin.to_string(),
            ));
        }
    } else {
        return Err(SchedulingError::ClientIsntFullyInitialized(
            command_instructions.origin.to_string(),
        ));
    }

    if let Some(sync) = state_manager.is_sync {
        if !sync {
            return Err(SchedulingError::ClientIsntFullyInitialized(
                command_instructions.origin.to_string(),
            ));
        }
    } else {
        return Err(SchedulingError::ClientIsntFullyInitialized(
            command_instructions.origin.to_string(),
        ));
    }

    // -- Extract client key

    let this_client_key_ref: String = if let Some(key) = &state_manager.key {
        key.clone()
    } else {
        return Err(SchedulingError::ClientIsntFullyInitialized(
            command_instructions.origin.to_string(),
        ));
    };

    // --  Check target != self and resolve command_target string

    let mut command_target: String;
    match command_instructions.target.clone() {
        CommandTarget::Origin => {
            return Err(SchedulingError::CantScheduleCommandsToItself(
                command_instructions.origin.to_string(),
            ));
        }
        CommandTarget::Host => command_target = "host".to_string(),
        CommandTarget::ClientKey(k) => {
            if &k == &this_client_key_ref {
                return Err(SchedulingError::CantScheduleCommandsToItself(
                    command_instructions.origin.to_string(),
                ));
            }
            command_target = k;
        }
    }

    // -- Validate network_map and handlers
    if let Some(network_map) = &mut state_manager.network_map {
        //> VERIFY IF THE COMMAND TARGET EXISTS
        match network_map.target_is_reachable(&command_target) {
            Ok(v) => {
                if !v {
                    return Err(SchedulingError::TargetDoesntExists(command_target));
                }
            }
            Err(_) => {
                return Err(SchedulingError::TargetDoesntExists(command_target));
            }
        }

        //> VERIFY IF THE HANDLER EXISTS IN THE TARGET
        if !network_map
            .handler_exists_in(command_target.as_str(), command_instructions.actf.as_str())
        {
            return Err(SchedulingError::HandlerDoesntExist(
                command_instructions.actf.clone(),
            ));
        }

        // -- Validate response_target (if any)
        if let Some(response_target) = command_instructions.response_target.clone() {
            match response_target {
                ResponseTarget::Origin => {
                    //* See if the handler exist here in origin

                    if let Some(this_node) = state_manager.client_node_configs {
                        let this_node_handlers = match this_node.get_node_handlers() {
                            Ok(n) => n,
                            Err(_) => {
                                return Err(SchedulingError::ClientIsntFullyInitialized(
                                    command_instructions.origin.to_string(),
                                ));
                            }
                        };

                        if command_instructions.collect_response {
                            // Only verify if response_actf exist is collect response is true
                            if let Some(response_actf) = command_instructions.response_actf.clone()
                            {
                                if response_actf != "".to_string() {
                                    //> See if this node has the expected handler
                                    if !this_node_handlers.contains_key(&response_actf) {
                                        return Err(SchedulingError::ResponseHandlerDoesntExist(
                                            response_actf.clone(),
                                        ));
                                    }
                                }
                            }
                        } else {
                            command_instructions.response_actf = Some("".to_string());
                            // If !collect_response resp_actf = ""
                        }
                    }
                }
                ResponseTarget::ClientKey(k) => {
                    //* See if target response is pointing to target
                    if command_target == k {
                        return Err(SchedulingError::TargetCantSendResponseToItself);
                    }

                    if command_instructions.collect_response {
                        // Only verify if response_actf exist is collect response is true
                        if let Some(response_actf) = command_instructions.response_actf.clone() {
                            if response_actf != "".to_string() {
                                if !network_map
                                    .handler_exists_in(k.as_str(), response_actf.as_str())
                                {
                                    return Err(SchedulingError::HandlerDoesntExist(
                                        response_actf.clone(),
                                    ));
                                }
                            }
                        } else {
                            //* Response actf is none then response will be ignored
                        }
                    } else {
                        return Err(SchedulingError::UnsuportedAction(format!("Can't send not autocollect response to {:?}, you can only send inplace responses to Origin!", k)));
                        // command_instructions.response_actf = Some("".to_string());
                        // If !collect_response resp_actf = ""
                    }
                }
                ResponseTarget::Host => {
                    //* See if the target is host and if the response is pointing to itself
                    if command_target == "host" {
                        return Err(SchedulingError::HostCantSendResponseToItself);
                    }

                    if command_instructions.collect_response {
                        // Only verify if response_actf exist is collect response is true
                        if let Some(response_actf) = command_instructions.response_actf.clone() {
                            if response_actf != "".to_string() {
                                if !network_map.handler_exists_in("host", response_actf.as_str()) {
                                    return Err(SchedulingError::HandlerDoesntExist(
                                        response_actf.clone(),
                                    ));
                                }
                            }
                        } else {
                            //* Response actf is none then response will be ignored
                        }
                    } else {
                        return Err(SchedulingError::UnsuportedAction("Can't send not autocollect response to host, it doesn't suports it yet!".to_string()));
                    }
                }
            }
        } else {
            //* If response target is none then response will be ignored
            // TODO >>> Verify it is correct!
        }
    } else {
        return Err(SchedulingError::ClientIsntFullyInitialized(
            command_instructions.origin.to_string(),
        ));
    }

    logger
        .debug(format!("[CLIENT][GLOBAL][Release] - CLIENT_ID"))
        .await;

    let client_key = state_manager.key.clone().unwrap();
    if client_key == "".to_string() {
        return Err(SchedulingError::ClientIsntFullyInitialized(
            command_instructions.origin.to_string(),
        ));
    }

    logger
        .debug(format!("Client id is: {:?}", client_key))
        .await;

    // -- Generate parity_id with direct `.await`

    let parity_id: String = enhanced_buffer::buffer_up_manager::buffer_up_gen_valid_parity_id(
        this_client_key_ref.clone(),
    )
    .await
    .map_err(SchedulingError::from)?;

    // -- Build and schedule the command
    let command = Command::new(
        client_key,
        parity_id.clone(),
        priority,
        command_instructions,
    );
    logger
        .debug(format!("[CLIENT] - Scheduling: {:?}", command))
        .await;

    let command_to_schedule: UpCommand = UpCommand::from_command(command);
    enhanced_buffer::buffer_up_manager::buffer_up_schedule(command_to_schedule.clone())
        .await
        .map_err(|e| SchedulingError::from(e))?;
    logger
        .info(format!("Command: {:?} scheduled!", command_to_schedule))
        .await;

    Ok(parity_id)
}
