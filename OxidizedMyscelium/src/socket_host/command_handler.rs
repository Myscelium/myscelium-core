use super::host_logger::log_handler::Logger;
use super::socket_host::{get_response, Response};
use crate::common::enhanced_buffer;
use crate::common::enhanced_buffer::utilities::{Command, CommandInstructions, CommandMode, CommandOrigin, CommandStatus, CommandTarget, CommandType};
use crate::socket_host::command_handler::enhanced_buffer::buffer_down_manager::DownCommand;
use crate::socket_host::command_handler::enhanced_buffer::buffer_up_manager::UpCommand;
use crate::socket_host::command_handler::enhanced_buffer::utilities::ResponseTarget;
use crate::socket_host::transposer_functions::handle_redirect::handle_redirect;
use crate::HOST_COMMAND_PATTERNS;
use crate::HOST_LOG_LEVEL;
use serde_json::{from_str, Value};
use std::collections::HashMap;

macro_rules! create_error_command_response {
    ($client_key:expr, $parity_id:expr, $error:expr) => {{
        let command_instructions = CommandInstructions::new(
            CommandMode::Response,
            CommandType::DirectFunction,
            CommandTarget::Origin,
            CommandStatus::Failure,
            CommandOrigin::Host,
            "error_handler".to_string(),
            HashMap::new(),
            $error.to_string(),
            None, // Not required here
            None, // Not required here
            None, // Not required here
            true,
        );

        let command = Command {
            client_key: $client_key.to_string(),
            parity_id: $parity_id.to_string(),
            priority: 11,
            command: command_instructions,
        };
        command
    }};
}

macro_rules! send_error_response {
    ($stream:expr, $command:expr, $logger:expr, $message:expr) => {
        let response = create_error_command_response!($command.client_key, $command.parity_id, $message);
        $logger.exception(format!("WARNING: {}, sending back: {:?}", $message, response));
        match send($stream, response) {
            Ok(_) => {},
            Err(e) => {
                handle_send_error!(e, $logger, $command.client_key);
                break;
            },
        }
    };
}

macro_rules! create_special_command_confirmation {
    ($client_key:expr, $command_parity_id:expr) => {{
        let conf_instruction = CommandInstructions::new(
            CommandMode::Response,
            CommandType::SpecialFunction,
            CommandTarget::Origin,
            CommandStatus::Success,
            CommandOrigin::Host,
            "C210".to_string(),
            HashMap::new(),
            "".to_string(),
            None, // Not required here
            None, // Not required here
            None, // Not required here
            true,
        );

        let resp = Command {
            client_key: $client_key.to_string(),
            parity_id: $command_parity_id.to_string(),
            priority: 11,
            command: conf_instruction,
        };

        resp
    }};
}

// {
//     Ok(_) => {},
//     Err(e) => handle_send_error!(e, logger, client_key),
// };
// handle_client_disconnect(&client_key);
// break;

macro_rules! acquire_logger {
    ($section_name:expr) => {{
        let host_log_level;
        {
            host_log_level = HOST_LOG_LEVEL.lock().clone();
        }
        Logger::new(host_log_level, $section_name)
    }};
}

// ->--------------------------------------------------------------------------------------------------------------
// -> INCOMMING REDIRECT COMMANDS PROCESSING

pub fn redirect_commands_processing(command: &Command, target: &String) -> Vec<Command> {
    // TODO >>> WHEN ADD THE PERMISSIONS ADD A MECHANISM TO CHECK IF THE CLIENT HAS PERMISSION TO ACCESS THIS ENDPOINTS

    let logger = acquire_logger!("Core");
    let mut client_key: String = "".to_string();

    let mut command_patterns;

    {
        command_patterns = HOST_COMMAND_PATTERNS.lock().clone();
    }

    logger.debug(format!("[HOST][REGIRSTRED PATTERNS]:\n{:?}", command_patterns));

    // > EARLY REMOVE FROM DOWN BUFFER TO AVOID REPETITION ERRORS SINCE THE COMMAND IS ALREADY BEING PROCESSED
    enhanced_buffer::buffer_down_manager::buffer_down_remove_schedule_by_parity_id(command.client_key.clone(), command.parity_id.clone());

    //> PREVIOUSLY CHECK REQUIREMENTS BEFORE REDIRECT
    if !command_patterns.target_is_reachable(target).unwrap() {
        let command: Command = create_error_command_response!(
            command.client_key.clone(),
            command.parity_id,
            format!("Function: {}, can be redirected because target: {} isn't reachable", command.command.actf, target)
        );
        logger.debug(format!("Sending back: {:?}", &command));
        let client_key = command.client_key.clone();
        return vec![command];
    }

    //> VERIFY IF THE TARGET IS SYNC
    if !command_patterns.target_is_ready(target).unwrap() {
        let command: Command = create_error_command_response!(
            command.client_key.clone(),
            command.parity_id,
            format!("Function: {}, can be redirected because target: {} isn't ready", command.command.actf, target)
        );
        logger.debug(format!("Sending back: {:?}", &command));
        let client_key = command.client_key.clone();
        return vec![command];
    }

    //> SEE IF THE HANDLER EXIST IN THE TARGET

    if command.command.mode == "Function" {
        if !command_patterns.handler_exists_in(target.as_str(), command.command.actf.as_str()) {
            let command: Command = create_error_command_response!(command.client_key.clone(), command.parity_id, format!("Function: {}, Doesn't exist in target client: {}!", command.command.actf, target));
            logger.debug(format!("Sending back: {:?}", &command));
            let client_key = command.client_key.clone();
            return vec![command];
        };
    }

    // -> When implementing permissions add the following rules:
    // TODO >>> See if the client has permission to send commands to this target

    //>--------------------------------------------------------------------------------------------------
    //> Response Target Rules

    //* Command Target should't be the same of the Response Target
    //* When a Client Sends a command the scheduler verify if the handler exists in itself
    //* Same happens for host

    // TODO >>> Add verification to cases where client is sendind a resp to host, verify if the resp_actf exists in target
    // TODO >>> ADD THIS TO THE OTHER CASES< NOT ONLY TO THE CommandTarget::ClientKey
    // TODO >>> See if is possible to reduce the nesting of this block, using assumptions where the Option is guaranteed to be some.
    // > Maybe early returns can help reduce nesting here.

    if let Some(response_target) = command.command.response_target.clone() {
        let resp_target = match response_target {
            ResponseTarget::Origin => "origin".to_string(),
            ResponseTarget::Host => "host".to_string(),
            ResponseTarget::ClientKey(key) => key,
        };

        //> IF TARGET IS EQUAL TO RESPONSE TARGET THEN RETURN A ERROR
        if &resp_target == target {
            let command: Command = create_error_command_response!(command.client_key.clone(), command.parity_id, format!("Can't send a response from target: {} to itself", target));
            logger.debug(format!("Sending back: {:?}", &command));
            let client_key = command.client_key.clone();
            return vec![command];
        }

        //> If resp target isn't origin, nor host then:
        if !vec!["origin", "host"].contains(&resp_target.as_str()) {
            let available_targets_map = command_patterns.get_node_keys().unwrap();
            let available_targets_keys: Vec<String> = available_targets_map.into_iter().map(|(_, value)| value).collect();

            //> CHECK IF THE TARGET EXISTS
            if !available_targets_keys.contains(&resp_target) {
                // If not exists
                let command: Command = create_error_command_response!(command.client_key.clone(), command.parity_id, format!("Response target: {} isn't reachable", &resp_target.as_str()));
                logger.debug(format!("Sending back: {:?}", &command));
                let client_key = command.client_key.clone();
                return vec![command];
            }

            //> Check if the handler to response exist in target (ONLY IF AUTO COLLECT == True)
            if command.command.collect_response {
                if let Some(response_actf) = command.command.response_actf.clone() {
                    if command.command.collect_response && response_actf != "" {
                        // Only verify if handler exists if auto collect response == true
                        if !command_patterns.handler_exists_in(resp_target.as_str(), response_actf.as_str()) {
                            let command: Command = create_error_command_response!(command.client_key.clone(), command.parity_id, format!("Response Handler: {}, Doesn't exist in target client: {}!", command.command.actf, target));
                            logger.debug(format!("Sending back: {:?}", &command));
                            let client_key = command.client_key.clone();
                            return vec![command];
                        };
                    }
                }
            }
        }
    }

    // TODO >>> Verify if the client that send this command has permission to send the response to this target

    //>--------------------------------------------------------------------------------------------------

    logger.debug(format!("Redirecting command to target: {}", target));

    let command_instructions_to_schedule: CommandInstructions = handle_redirect(&command.command.clone(), &mut command.client_key.clone(), command.parity_id.clone(), command.priority.clone());

    //> CAST COMMAND TO REDIRECT
    let command_to_redirect: Command = Command {
        client_key: target.to_string().clone(),
        parity_id: command.parity_id.to_string().clone(),
        priority: 11,
        command: command_instructions_to_schedule,
    };

    // > VERIFY IF ALREADY PROCESSED:
    logger.debug("Command is in command patterns!".to_string());
    let command_is_not_registry: bool = enhanced_buffer::buffer_up_manager::check_if_parity_id_is_registered(command_to_redirect.parity_id.clone(), command_to_redirect.client_key.clone());
    let mut response: Vec<Command> = vec![];

    //> HANDLE COMMANDS WITH RESPONSE:
    if !command_is_not_registry {
        logger.warn(format!("Command {}, already have a response!", command.parity_id.clone()));
        match get_response(command.clone()) {
            Response::Command(c) => {
                if c.client_key == command.client_key {
                    response.push(c);
                } else {
                    logger.info("Response is None!".to_string());
                    response.push(create_special_command_confirmation!(command.client_key.clone(), command.parity_id.clone()));
                }
            },
            Response::None => {
                logger.info("Response is None!".to_string());
                response.push(create_special_command_confirmation!(command.client_key.clone(), command.parity_id.clone()));
            },
        }

    //> HANDLE COMMANDS WITHOUT RESPONSES:
    } else {
        // _ = handle_common_function(&command_to_redirect);
        let up_command = UpCommand::from_command(command_to_redirect.clone());
        enhanced_buffer::buffer_up_manager::buffer_up_schedule(up_command);
        response.push(create_special_command_confirmation!(command.client_key.clone(), command.parity_id.clone()));
    }

    //> SEND RESPONSE BACK - HERE IT CAN BE COMMAND RESPONSES OR CONFIRMATIONS
    logger.debug(format!("Sending back: {:?}", response));
    return response;
}

// ->--------------------------------------------------------------------------------------------------------------
// -> INCOMMING HOST COMMANDS PROCESSING

/// Handles common commands that don't fall under special functions.
///
/// This function schedules the command for processing using the `buffer_down_manager`. After scheduling, it sends a "C210" special command as a receive confirmation.
///
/// # Parameters
/// - `command`: The `Command` object that needs to be handled.
///
/// # Returns
/// - A `Command` object representing the response for the common command.
fn handle_common_function(command: &Command) -> Command {
    // >--------------------------------------------------------------------------------------------------------------
    // > Schedule to process

    let json_command = serde_json::to_string(&command.command).unwrap();
    let down_command = DownCommand::new(
        command.client_key.clone(),
        command.parity_id.clone(),
        command.priority,
        json_command,
        command.command.mode.clone(),
        command.command.collect_response.clone(),
    );

    enhanced_buffer::buffer_down_manager::buffer_down_schedule(&down_command);

    // >--------------------------------------------------------------------------------------------------------------
    // > Send receive conf

    let kwargs: HashMap<String, Value> = HashMap::new();

    let command_instructions: CommandInstructions = CommandInstructions::new(
        CommandMode::Function,
        CommandType::SpecialFunction,
        CommandTarget::Origin,
        CommandStatus::Success,
        CommandOrigin::Host,
        "C210".to_string(),
        kwargs,
        "".to_string(),
        None, // Not required here
        None, // Not required here
        None, // Not required here
        true,
    );

    let conf_command = Command {
        client_key: command.client_key.to_string().clone(),
        parity_id: command.parity_id.to_string().clone(),
        priority: 11,
        command: command_instructions,
    };

    return conf_command;
}

pub fn host_commands_processing(command: &Command) -> Command {
    let logger = acquire_logger!("Core");
    let mut client_key: String = "".to_string();

    let mut command_patterns;

    {
        command_patterns = HOST_COMMAND_PATTERNS.lock().clone();
    }

    logger.debug(format!("[HOST][REGIRSTRED PATTERNS]:\n{:?}", command_patterns));

    let direct_functions: Vec<String> = vec!["get_registered_commands", "update_client_commands_ref", "restrictive_update_client_commands_ref", "add_client", "update_client", "remove_client"]
        .into_iter()
        .map(|s| s.to_string())
        .collect();

    //> CHECK IF HANDLER DON'T EXIST AND RETURN & SEND ERROR MESSAGE IF NOT
    if !command_patterns.handler_exists_in("host", command.command.actf.as_str()) && !direct_functions.contains(&command.command.actf) {
        let command: Command = create_error_command_response!(command.client_key.clone(), command.parity_id, format!("Function: {}, Doesn't exist in host callbacks nor in any client!", command.command.actf));
        logger.debug(format!("Sending back: {:?}", &command));
        let client_key = command.client_key.clone();
        return command;
    };

    // > VERIFY IF ALREADY PROCESSED:
    logger.debug("Command is in command patterns!".to_string());
    let command_is_not_registry: bool = enhanced_buffer::buffer_up_manager::check_if_parity_id_is_registered(command.parity_id.clone(), command.client_key.clone());
    let response: Command;

    //> HANDLE COMMANDS WITH RESPONSE:
    if !command_is_not_registry {
        logger.warn(format!("Command {}, already have a response!", command.parity_id.clone()));
        let response = match get_response(command.clone()) {
            Response::Command(c) => {
                if c.client_key == command.client_key {
                    c
                } else {
                    logger.info("Response is None!".to_string());
                    create_special_command_confirmation!(command.client_key.clone(), command.parity_id.clone())
                }
            },
            Response::None => {
                logger.info("Response is None!".to_string());
                create_special_command_confirmation!(command.client_key.clone(), command.parity_id.clone())
            },
        };

        return response;
    }

    //> HANDLE COMMANDS WITHOUT RESPONSES:

    //> If Response target is defined
    if let Some(response_target) = command.command.response_target.clone() {
        let resp_target = match response_target {
            ResponseTarget::Origin => "origin".to_string(),
            ResponseTarget::Host => "host".to_string(),
            ResponseTarget::ClientKey(key) => key,
        };

        // > Check if the response target is host, if so return error (don't allow send commands to self)
        if resp_target == "host" {
            let command: Command = create_error_command_response!(
                command.client_key.clone(),
                command.parity_id,
                format!(
                    "Can't send a response from command: {:?} processed in host to a host response handler: {:?}, this is a self conflic!",
                    command.command.actf, command.command.response_actf
                )
            );
            logger.debug(format!("Sending back: {:?}", &command));
            let client_key = command.client_key.clone();
            return command;
        }

        //> If resp target isn't origin, nor host then:
        if vec!["origin", "host"].contains(&resp_target.as_str()) {
            response = handle_common_function(&command);
            return response;
        }

        let available_targets_map = command_patterns.get_node_keys().unwrap();
        let available_targets_keys: Vec<String> = available_targets_map.into_iter().map(|(_, value)| value).collect();

        //> CHECK IF THE TARGET EXISTS
        if !available_targets_keys.contains(&resp_target) {
            let command: Command = create_error_command_response!(command.client_key.clone(), command.parity_id, format!("Response target: {} isn't reachable", &resp_target.as_str()));
            logger.debug(format!("Sending back: {:?}", &command));
            return command;
        }

        //> Check if the target is ready
        // TODO >>> Possible wait to target become ready
        if !command_patterns.target_is_ready(&resp_target).unwrap() {
            let command: Command = create_error_command_response!(command.client_key.clone(), command.parity_id, format!("Response target: {} isn't ready yet", &resp_target.as_str()));
            logger.debug(format!("Sending back: {:?}", &command));
            return command;
        }

        //> Check if the handler to response exist in target (this also will handle the case that the target isn't initialized)
        if let Some(response_actf) = command.command.response_actf.clone() {
            if command.command.collect_response && response_actf != "" {
                // Only verify if response actf exists if collect response == true
                if !command_patterns.handler_exists_in(resp_target.as_str(), response_actf.as_str()) {
                    let command: Command = create_error_command_response!(
                        command.client_key.clone(),
                        command.parity_id,
                        format!("Response Handler: {}, Doesn't exist in target client: {}!", command.command.actf, resp_target)
                    );
                    logger.debug(format!("Sending back: {:?}", &command));
                    return command;
                }
            }
        }
    };

    response = handle_common_function(&command);
    return response;
}
