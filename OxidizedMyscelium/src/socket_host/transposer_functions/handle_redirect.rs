use serde_json::Value;
use std::collections::HashMap;

use crate::common::enhanced_buffer::utilities::{Command, CommandInstructions, CommandMode, CommandOrigin, CommandStatus, CommandTarget, CommandType};

use crate::common::client_manager::manager::check_if_client_key_exists;

use crate::common::enhanced_buffer;
use crate::common::enhanced_buffer::buffer_down_manager::DownCommand;
use crate::common::enhanced_buffer::buffer_up_manager::UpCommand;

use crate::common::functions::converters::convert_to_value_map;

use crate::common::structs::results_structs::ResultType;

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

//> ------------------------------------------------------------------------------------------------------------------------------------------------
//> Handle Redirect

/// Handles redirection logic for incoming commands.
///
/// This function processes an incoming command, checks if it contains
/// the necessary keys for redirection, and updates the client ID to which
/// future commands will be sent. It also schedules an `UpCommand` based on
/// the provided `DownCommand`.
///
/// # Parameters
///
/// - `m`: A `HashMap` representing the incoming command. It should contain
///   keys and values represented as `ResultType` variants.
/// - `client_id`: A mutable reference to the client ID. This ID will be updated
///   if the redirection is successful.
/// - `down_command`: The `DownCommand` based on which an `UpCommand` will be scheduled.
///
/// # Returns
///
/// A `HashMap` containing the response. If there's an error during the processing,
/// an error response will be returned with a corresponding message.
///
/// # Errors
///
/// The function can return error responses in the following scenarios:
///
/// - The incoming command does not contain the "redirect_to" key.
/// - The specified client to redirect to does not exist.
/// - The incoming command does not contain the "kwargs" key.
///
/// # Panics
///
/// This function can panic in the following scenarios (due to `unwrap` calls):
///
/// - The `redirect_to` or `response_activation_function` values are not present in `converted_m`.
/// - The `redirect_to` or `response_activation_function` values cannot be deserialized to a `String`.
///
/// # Examples
///
/// ```rust
/// let mut client_id = "client123".to_string();
/// let down_command = DownCommand::new(..=); // Initialize a DownCommand
/// let m = ...; // Initialize the HashMap command
///
/// let response = handle_redirect(m, &mut client_id, down_command);
/// ```
///
pub fn handle_redirect(m: &CommandInstructions, client_id: &mut String, parity_id: String, priority: u8) -> CommandInstructions {
    let logger = acquire_logger!("[Process][Handle Redirect]");

    println!("Try to redirect: {:?}", m);

    // if m.command_type != CommandType::Redirect {
    //     logger.warn("Error! Callback response args don't have redirect_to client_id field!".to_string());
    //     return create_error_response_and_return!("Error! Callback response args don't have redirect_to client_id field!");
    //     // error_response!("Error! Callback response args don't have redirect_to client_id field!");
    // }

    // -> Filter not allowed cases:
    let redirect_to = match &m.target {
        CommandTarget::Host => {
            logger.warn("Error! Cont redirect from origin to host, this is a Origin to Host direct case!".to_string());
            return create_error_response_and_return!("Error! Cont redirect from origin to host, this is a Origin to Host direct case!");
        },
        CommandTarget::Origin => {
            logger.warn("Error! Cont redirect from host to origin, this is a host to origin direct case!".to_string());
            return create_error_response_and_return!("Error! Cont redirect from host to origin, this is a Origin to Host direct case!");
        },
        CommandTarget::ClientKey(c) => c.clone(),
    };

    if !check_if_client_key_exists(redirect_to.clone()) {
        logger.warn(format!("Error! request to redirect to client_id: {} failed, client doesn't exist!", redirect_to));
        return create_error_response_and_return!(format!("Error! request to redirect to client_id: {} failed, client doesn't exist!", redirect_to));
        // return error_response!(format!("Error! request to redirect to client_id: {} failed, client doesn't exist!", redirect_to.to_string()));
    }

    //> This was remove because in the cases that sends a lot of redirect this makes a spamming into the client that sends the list to retransmit:
    // let mut command_map = HashMap::new();
    // command_map.insert("command_type".to_string(), Value::String("special_function".to_string()));
    // command_map.insert("function".to_string(), Value::String("C210".to_string()));
    // let response = serde_json::to_string(&command_map).unwrap();

    // let up_command = UpCommand::new(client_id.clone(), parity_id.clone(), priority.clone(), response);
    // enhanced_buffer::buffer_up_manager::buffer_up_schedule(up_command);

    // TODO >>> Add a logic here to see when the redirect is to redirect a `update_available_host_commands` command and use this as a function and set response mode to to host
    // TODO >>> See if need to add restrictions to some command modes and types don't be allwoed to be redirectd

    let new_command_instructions = CommandInstructions::new(
        m.mode.clone(),         // Depend on the command sended
        m.command_type.clone(), // Depends on the command sended
        CommandTarget::Origin,  // Seted because at this point this was alwready redirected
        CommandStatus::Success,
        CommandOrigin::ClientKey(client_id.clone()),
        m.actf.clone(),
        m.kwargs.clone(),
        "".to_string(),
    );

    *client_id = redirect_to; // > Update the client id that it will send to

    return new_command_instructions;
}
