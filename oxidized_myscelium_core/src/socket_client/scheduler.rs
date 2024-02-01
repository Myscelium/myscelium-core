use crate::common::enhanced_buffer;
use crate::common::enhanced_buffer::buffer_down_manager::DownCommand;
use crate::common::enhanced_buffer::buffer_up_manager::UpCommand;
use crate::common::enhanced_buffer::utilities::{Command, CommandInstructions, CommandType};
use crate::common::functions::advanced_lockers::smart_lock;
use crate::socket_client::states_manager::manager::ClientState;

use lazy_static::lazy_static;

use serde_json::{from_str, Value};
use std::collections::HashMap;

use super::client_logger::log_handler::Logger;
use crate::{CLIENT_LOG_LEVEL, CLIENT_STATE_MANAGER};

use parking_lot::Mutex;

use std::sync::Arc;

// lazy_static! {
//    static ref CLIENT_ID: Arc<Mutex<String>> = Arc::new(Mutex::new(' '.to_string()));
// }

macro_rules! acquire_logger {
    ($section_name:expr) => {{
        let client_log_level;
        {
            client_log_level = CLIENT_LOG_LEVEL.lock().clone();
        }
        Logger::new(client_log_level, $section_name)
    }};
}

/// Sets the global client ID to the specified value.
///
/// The client ID is a unique identifier that represents the client in the communication process.
/// This function updates the global `CLIENT_ID` variable to the provided value.
///
/// # Arguments
/// - `client_uid`: The new client ID to be set.
//pub fn set_client_id(client_uid: String) {
//    println!("Setting client_id to: {:?}", client_uid.clone());
//
//    // let client_key_storage = &CLIENT_ID;
//    // smart_lock(client_key_storage, |key: &mut String| {
//    //     *key = client_uid;
//    // });
//
//    println!("[CLIENT][GLOBAL][Try Lock] - CLIENT_ID");
//    {
//        let mut key = CLIENT_ID.lock(); // TODO > This is using parking lot, see if need to change to smart-lock
//        println!("[CLIENT][GLOBAL][Lock] - CLIENT_ID");
//        *key = client_uid
//    }
//    println!("[CLIENT][GLOBAL][Release] -  CLIENT_ID");
//}

/// Requests the available commands that are registered on the host.
///
/// This function prepares a command request for the host to retrieve the list of
/// registered commands. The constructed request is then scheduled for processing.
// pub fn request_host_available_commands() {
//     let mut request_host_commands: HashMap<String, String> = HashMap::new();
//     request_host_commands.insert("function".to_string(), "get_registered_commands".to_string());
//     request_host_commands.insert("command_type".to_string(), "function".to_string());
//     request_host_commands.insert("kwargs".to_string(), "{}".to_string());

//     schedule(request_host_commands, 11)
// }

pub enum SchedulingError {
    ClientIsntFullyInitialized,
    CantReadStates,
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
pub fn schedule(
    command_instructions: CommandInstructions,
    priority: u8,
) -> Result<(), SchedulingError> {
    let logger: Logger = acquire_logger!("Core - Scheduler");

    logger.debug("Enter Scheduler".to_string());

    println!("[CLIENT][GLOBAL][Try Lock] - CLIENT_ID");

    let state_manager = match ClientState::load_from_storage() {
        Ok(s) => s,
        Err(_) => return Err(SchedulingError::CantReadStates),
    };

    // if !state_manager.is_fully_initialized() {
    //    return Err(SchedulingError::ClientIsntFullyInitialized);
    //}

    if let Some(ready) = state_manager.is_ready {
        if !ready {
            return Err(SchedulingError::ClientIsntFullyInitialized);
        }
    } else {
        return Err(SchedulingError::ClientIsntFullyInitialized);
    }

    if let Some(sync) = state_manager.is_sync {
        if !sync {
            return Err(SchedulingError::ClientIsntFullyInitialized);
        }
    } else {
        return Err(SchedulingError::ClientIsntFullyInitialized);
    }

    // {
    //    let key = CLIENT_ID.lock(); // TODO > This is using parking lot, see if need to change to smart-lock
    //    println!("[CLIENT][GLOBAL][Lock] - CLIENT_ID");
    //    client_key = key.clone();
    //    drop(key)
    // }

    println!("[CLIENT][GLOBAL][Release] - CLIENT_ID");

    let client_key = state_manager.key.clone().unwrap();

    if client_key == "".to_string() {
        return Err(SchedulingError::ClientIsntFullyInitialized);
    }

    logger.debug(format!("Client id is: {:?}", client_key));

    // TODO >>> Add mecanisms to check the structure of the command that we are trying to registry

    let parity_id: String =
        enhanced_buffer::buffer_up_manager::buffer_up_gen_valid_parity_id(client_key.clone());

    let command = Command::new(client_key, parity_id, priority, command_instructions);

    println!("[CLIENT] - Scheduling: {:?}", command);

    let command_to_schedule: UpCommand = UpCommand::from_command(command);

    enhanced_buffer::buffer_up_manager::buffer_up_schedule(command_to_schedule.clone());

    logger.info(format!("Command: {:?} scheduled!", command_to_schedule));

    Ok(())
}
