use crate::common::enhanced_buffer;
use crate::common::enhanced_buffer::buffer_down_manager::DownCommand;
use crate::common::enhanced_buffer::buffer_up_manager::UpCommand;
use crate::common::enhanced_buffer::utilities::{Command, CommandInstructions, CommandMode, CommandOrigin, CommandStatus, CommandTarget, CommandType};
use crate::common::functions::callbacks::call_callback;
use crate::common::structs::available_commands::NetworkMap;
use crate::socket_client::functions::direct_functions::handle_direct_function;
use crate::socket_host::transposer_functions::handle_direct_function::ProcessResult;

use indexmap::IndexMap;
use lazy_static::lazy_static;
use parking_lot::{Mutex, MutexGuard};
use serde_json::Value;

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crate::{CLIENT_IS_RUNNING, CLIENT_NODE_CONFIGS, HOST_ALLOWED_COMMANDS};

use super::client_logger::log_handler::Logger;
use crate::CLIENT_LOG_LEVEL;

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

use crate::CLIENT_NODE_KEY;

use std::any::Any;
use std::boxed::Box;

type Callback = dyn Fn(&[&dyn Any]) -> Box<dyn Any> + Send + Sync;

use crate::common::structs::callbacks::{CallbackClosure, MyCallbacks};
use crate::CLIENT_CALLBACK_PATTERNS;

lazy_static! {
    static ref NUM_WORKERS: Arc<Mutex<u32>> = Arc::new(Mutex::new(5));
}

/// Sets the number of worker threads for the socket client transposer.
///
/// This function sets the number of workers for both the down buffer manager
/// and the up buffer manager. Each manager's `set_workers_num` method is
/// called with the specified number of workers.
///
/// # Arguments
/// - `n_workers`: The desired number of worker threads for the transposer.
pub fn set_socket_client_transposer_workers_num(n_workers: u32) {
    let logger = acquire_logger!("Transposer");
    {
        logger.debug(format!("[CLIENT][GLOBAL][Try Lock] - NUM_WORKERS"));
        let mut default_num_of_workers = NUM_WORKERS.lock();
        logger.debug(format!("[CLIENT][GLOBAL][Lock] - NUM_WORKERS"));
        *default_num_of_workers = n_workers;
    }

    logger.debug(format!("[CLIENT][GLOBAL][Release] - NUM_WORKERS"));

    enhanced_buffer::buffer_down_manager::set_workers_num(n_workers);
    enhanced_buffer::buffer_up_manager::set_workers_num(n_workers);
}

/// Sets the command patterns and callback patterns for the socket client transposer.
///
/// The command patterns define the recognized commands and their structures, while
/// the callback patterns define the associated Python functions and their arguments
/// that should be executed for each recognized command.
///
/// # Arguments
/// - `commands_patterns`: A map of recognized command patterns.
/// - `callbacks_patterns`: A map of associated Python functions and arguments for each recognized command.
pub fn set_socket_client_transposer_callbacks(key: String, callback: Box<CallbackClosure>) {
    let logger = acquire_logger!("Transposer");
    logger.debug(format!("[CLIENT][GLOBAL][Try Lock] - CLIENT_CALLBACK_PATTERNS"));
    let patterns = &CLIENT_CALLBACK_PATTERNS;
    logger.debug(format!("[CLIENT][GLOBAL][Lock] - CLIENT_CALLBACK_PATTERNS"));
    patterns.insert(key, callback);
    logger.debug(format!("[CLIENT][GLOBAL][Release] - CLIENT_CALLBACK_PATTERNS"));
}

// Transposer:

/// Represents possible errors that can occur during the processing of commands by the transposer.
///
/// This enumeration is used to categorize and communicate specific error conditions
/// that can arise when the transposer attempts to process a command.
pub enum ProcessError {
    /// Indicates that the command has already been processed.
    CommandAlreadyProcessed(String),

    /// Indicates that the command is missing a required "function" attribute.
    MissingCommandFunction(String),

    /// Indicates that the command is not recognized because it's not registered in the command patterns.
    CommandNotRegistered(String),

    /// Indicates that the callback response for a command is invalid.
    InvalidCallbackResponse(String, String),

    /// Represents a generic error condition with an associated error message.
    Error(String),

    /// Indicates that the type of the command is unknown or not recognized.
    UnknownCommandType,

    /// Indicates that a response key is missing from a command.
    MissingResponseKey(String),

    // Indicates that a kwargs key is missing from a command.
    MissingKwargsKey(String),
}

// -> One idea is to create a obligatory key in the command.command and instead of only function create a type kwarg field
// > Type can be:
// >    - same as origin
// >    - redirect

// > if it is redirect one extra kwarg is necessary that have the client_key to redirect
// * This will create a need to have a local database in the host to store the clients
// * and to store when is the last contact of some client, if it is some threshold value
// * more it will remove the registered client, if it have a contact recent, this will redirect the message
// * however if the message is becomes too old before the client the message is redirected catches it
// * The system have to remove this old message from the buffer too.

/// Processes a down command using the specified Python environment and returns the result.
///
/// This function performs the following operations:
/// - Verifies that the command hasn't been processed already.
/// - Determines the type of the command (function, response, error, redirect, unknown).
/// - Checks if the command is a special "update available host commands" command.
/// - Validates the command against known command patterns.
/// - Calls the appropriate Python callback function for the command.
/// - Processes the Python callback's return value.
/// - Removes the processed command from the down buffer schedule.
/// - Schedules the resulting up command for transmission.
///
/// # Arguments
/// - `py`: The Python environment to use for executing Python code.
/// - `down_command`: The command to process.
///
/// # Returns
/// - `Ok(())` if the command was processed successfully.
/// - `Err(ProcessError)` if an error occurred during processing.
fn process(down_command: &DownCommand, client_key: &String) -> Result<(), ProcessError> {
    let logger = acquire_logger!("Transposer - Process");

    logger.info(format!("Initializing processing!"));

    // Check if the command has already been registered in the up buffer
    let command_is_not_registry: bool = enhanced_buffer::buffer_up_manager::check_if_parity_id_is_registered(down_command.parity_id.clone(), down_command.client_key.clone());
    let command_id: u32 = down_command.command_id.unwrap().clone();

    {
        if !command_is_not_registry {
            // If command is already registered, remove it from the down buffer schedule
            enhanced_buffer::buffer_down_manager::buffer_down_remove_schedule_by_id(command_id);
            return Err(ProcessError::CommandAlreadyProcessed(down_command.parity_id.clone()));
        }
    }

    // TODO >>> Use the command.command or create a require type field to redirect the command to another client

    // Convert the down command to a more general command structure for further processing
    let translated_command: Command = match Command::from_down_command(down_command) {
        Ok(c) => c.clone(),
        Err(e) => {
            logger.debug(format!("error converting down_command into command: {:?}", e));
            return Err(ProcessError::Error(format!("{:?}", e)));
        },
    };

    logger.debug(format!("Translated command: {:?}", translated_command));

    // logger.info(format!("Command function: {} is a valid function!", activation_key));

    let client_key = translated_command.client_key.clone();
    logger.debug(format!("Client key is: {:?}", client_key));

    // let direct_functions: Vec<String> = vec!["update_available_host_commands", "get_socket_client_available_handlers"].into_iter().map(|s| s.to_string()).collect();

    let resp: ProcessResult;
    logger.debug(format!("Command is a direct function: {:?}", translated_command.command.command_type == "DirectFunction"));

    if translated_command.command.command_type == "DirectFunction" {
        logger.debug(format!("Command is a direct function!"));
        logger.info(format!("Command function: {} is a valid function!", translated_command.command.actf));
        resp = handle_direct_function(&translated_command.command, &client_key, command_id)?.clone(); // This cloen avoids locking
        logger.debug(format!("Direct Function Result: {:?}", resp));
    } else {
        logger.debug(format!("Command isn't a direct function"));

        logger.debug(format!("Calling the callback!\n"));
        // Execute the associated Python callback for the command

        // -> EXTRACT CALLBACK FUNCTION AND CALL IT
        let response;

        {
            // > THIS WAS DONE THIS WAY TO BE ABLE TO USE MULTITHREADING WITH HIGH INTENSIVE FUNCTION WITHOUT ANY PROBLEM
            let callback_patterns = CLIENT_CALLBACK_PATTERNS.clone();

            let command_instructions = translated_command.command.clone();
            let kwargs_to_call: HashMap<String, Value>;
            let mut map = HashMap::new();

            // > Get the info parameters and add to kwargs
            map.insert("mode".to_string(), serde_json::to_value(&command_instructions.mode).unwrap());
            map.insert("status".to_string(), serde_json::to_value(&command_instructions.status).unwrap());
            map.insert("origin".to_string(), serde_json::to_value(&command_instructions.origin).unwrap());
            map.insert("message".to_string(), serde_json::to_value(&command_instructions.message).unwrap());
            map.insert("auto_collect".to_string(), serde_json::to_value(&command_instructions.collect_response).unwrap());
            map.insert("response_actf".to_string(), serde_json::to_value(&command_instructions.response_actf).unwrap());
            map.insert("response_type".to_string(), serde_json::to_value(&command_instructions.response_type).unwrap());
            map.insert("response_target".to_string(), serde_json::to_value(&command_instructions.response_target).unwrap());

            let mut kwargs = command_instructions.kwargs.clone();
            kwargs.insert("info".to_string(), serde_json::to_value(map).unwrap());

            logger.debug(format!("kwargs to pass to external function: {:?}", kwargs));

            //> Get the Node Configs, Here in client we can directly acess it
            let mut args_pattern: IndexMap<String, String> = IndexMap::new();

            {
                let mut global_command_patterns = CLIENT_NODE_CONFIGS.lock();
                let host_handlers = global_command_patterns.get_node_handlers().unwrap();
                let target_handler_params = host_handlers.get(&command_instructions.actf.clone()).unwrap();

                //Obtain the correct order of the kwargs
                args_pattern = target_handler_params.clone();
            }

            //> Call the callback
            response = match callback_patterns.call(translated_command.command.clone().actf.as_str(), kwargs, args_pattern) {
                Ok(r) => {
                    logger.info(format!("External function: {} is a valid function!", translated_command.command.actf.clone()));
                    r
                },
                Err(e) => {
                    // Existing logic to handle the error
                    logger.exception(format!("Callback error: {:?}", e));
                    return Err(ProcessError::Error(format!("{:?}", e)));
                },
            };
        }

        // -> PROCESS CALLBACK RESPONSE:
        resp = match response.downcast::<CommandInstructions>() {
            Ok(instructions_box) => {
                // Successfully downcasted, instructions_box is now a Box<CommandInstructions>
                logger.debug(format!("Successfully downcasted!"));
                // You can now use instructions_box as Box<CommandInstructions>
                let instruction = *instructions_box;
                ProcessResult::CommandInstructions(instruction)
            },
            Err(e) => {
                // The downcast operation failed
                logger.debug(format!("Failed to downcast callback response! Error: {:?}", e));
                ProcessResult::Error("Failed to downcast callback response!".to_string())
            },
        };
    }

    let client_key = down_command.client_key.clone();

    // TODO >>> Add a rule to command that the origin isn't host that give a error be redirected to origin
    // TODO >>> Remake the command, in a way that it accept Values instead of only string, to we be able to use Value map instead of a json str
    //> This will allow to easily manage commands, reducing the times that it needs to be parsed from str and allowing convert from value directly.

    logger.debug(format!("Function returned: {:?}", resp));
    logger.info(format!("Command: {:?}, processed!", down_command.parity_id.clone()));

    match resp {
        ProcessResult::CommandInstructions(c) => {
            logger.debug(format!("Received response: {:?}", c));
            let command: Command = Command::new(client_key.clone(), down_command.parity_id.clone(), down_command.priority.clone(), c);
            let up_command: UpCommand = UpCommand::from_command(command);
            enhanced_buffer::buffer_down_manager::buffer_down_remove_schedule_by_id(command_id.clone());
            enhanced_buffer::buffer_up_manager::buffer_up_schedule(up_command);
        },
        ProcessResult::List(l) => {
            for c in l {
                match c {
                    ProcessResult::Error(e) => {
                        logger.debug(format!("Receive a error: {:?}", e));
                    },
                    ProcessResult::Empty => {
                        logger.debug(format!("Response is empty, continuing!"));
                    },
                    ProcessResult::List(_) => {
                        logger.debug(format!("Receive a ilegal process Result List inside a Process Resul List!"));
                    },
                    ProcessResult::CommandInstructions(c) => {
                        let command: Command = Command::new(client_key.clone(), down_command.parity_id.clone(), down_command.priority.clone(), c);
                        let up_command: UpCommand = UpCommand::from_command(command);
                        enhanced_buffer::buffer_up_manager::buffer_up_schedule(up_command);
                    },
                }
                enhanced_buffer::buffer_down_manager::buffer_down_remove_schedule_by_id(command_id.clone());
            }
        },
        ProcessResult::Error(e) => {
            logger.debug(format!("Receive a error: {:?}", e));
            enhanced_buffer::buffer_down_manager::buffer_down_remove_schedule_by_id(command_id.clone());
        },
        ProcessResult::Empty => {
            logger.debug(format!("Response is empty, continuing!"));
            enhanced_buffer::buffer_down_manager::buffer_down_remove_schedule_by_id(command_id.clone());
        },
    }

    return Ok(());
}

/// Clears old data from the buffer.
///
/// This function invokes methods from `buffer_down_manager` and `buffer_up_manager`
/// to clear old commands from both up and down buffers.
fn clear_old_data() {
    enhanced_buffer::buffer_down_manager::buffer_down_clear_old_commands();
    enhanced_buffer::buffer_up_manager::buffer_up_clear_old_commands();
}

/// Initializes the socket client transposer.
///
/// This function manages the client-side data processing workflow. It retrieves scheduled commands
/// from the down buffer, processes them, and ensures that they are correctly formatted for transmission.
/// The function prioritizes commands based on their priority and processes them accordingly.
///
/// # Notes
/// - If the client is not running, the transposer shuts down.
/// - If there are no scheduled commands, the function clears old data and goes to sleep for 500ms.
/// - The function uses a logger named "Transposer" for logging various stages of the process.
pub fn initialize_socket_client_transposer() {
    let logger = acquire_logger!("Transposer");

    // Retrieve scheduled commands
    let mut schedule: Vec<DownCommand> = enhanced_buffer::buffer_down_manager::buffer_down_list_schedule();

    // -> Sort commands by priority in ascending order
    schedule.sort_by(|a, b| b.priority.cmp(&a.priority));

    // -> Filter only auto collect == true or any that is diferent of a Response (Only responses have auto collect == false)
    schedule = schedule.into_iter().filter(|s| s.command_mode != "Response" || s.auto_collect).collect();

    logger.debug(format!("\nSchedule to process:\n{:?}\n", schedule));

    let schedule_len = schedule.len();

    // If there are no commands to process, clear old data and sleep
    if !(schedule_len > 0) {
        logger.debug(format!("Nothing in the schedule, skipping >>>"));
        clear_old_data();
        thread::sleep(Duration::from_millis(100));
        return;
    } else {
        if schedule_len > 1 {
            logger.debug(format!("Find {} commands to process", schedule_len))
        } else {
            logger.debug(format!("Find {} command to process", 1))
        }
    }

    logger.info(format!("\nData found in schedule!"));

    // Validate the command against known command patterns
    let client_key;

    logger.debug(format!("[CLIENT][GLOBAL][Try Lock] - CLIENT_NODE_KEY"));
    {
        let client_n = CLIENT_NODE_KEY.lock();
        logger.debug(format!("[CLIENT][GLOBAL][Lock] - CLIENT_NODE_KEY"));
        client_key = client_n.clone();
    }
    logger.debug(format!("[CLIENT][GLOBAL][Release] - CLIENT_NODE_KEY"));

    // Process each scheduled command
    for dow_command in schedule {
        let logger = acquire_logger!("Transposer");

        // -> Check if command isn't a inplace response
        if dow_command.command_mode == "Response" && !dow_command.auto_collect {
            continue;
        }

        logger.info(format!("Get a pool worker in transposer!"));

        {
            logger.debug(format!("Acquired Python in a process task!"));

            // Process the command and handle potential errors
            let result = process(&dow_command, &client_key).map_err(|e| match e {
                ProcessError::CommandAlreadyProcessed(m) => {
                    format!("Command: {:?} already processed! So skipping", m)
                },
                ProcessError::CommandNotRegistered(m) => {
                    format!("Command function {:?} not registered in the callbacks! So skipping", m)
                },
                ProcessError::MissingResponseKey(m) => {
                    format!("Command: {:?}, missing command response key", m)
                },
                ProcessError::MissingKwargsKey(m) => {
                    format!("Command: {:?}, missing command kwargs key", m)
                },
                ProcessError::MissingCommandFunction(m) => {
                    format!("Command: {:?}, missing command function", m)
                },
                ProcessError::InvalidCallbackResponse(m, r) => {
                    format!("Callback function: {:?} invalid response: {:?}", m, r)
                },
                ProcessError::Error(e) => {
                    format!("An error occurred while processing command: {:?}", e)
                },
                ProcessError::UnknownCommandType => "Unknown Command type".to_string(),
            });

            match result {
                Ok(()) => {
                    logger.info(format!("Finalized a process task!"));
                },
                Err(e) => {
                    logger.warn(format!("\nWarning: {:?}\n", e));
                },
            }
        }
    }
}
