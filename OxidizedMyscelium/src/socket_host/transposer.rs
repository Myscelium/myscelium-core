use lazy_static::lazy_static;
use serde_json::{from_str, from_value, Value};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::thread;

use super::host_logger;
use super::host_logger::log_handler::Logger;
use super::transposer_functions::handle_direct_function::ProcessResult;
use crate::common::enhanced_buffer;
use crate::common::enhanced_buffer::buffer_down_manager::DownCommand;
use crate::common::enhanced_buffer::buffer_up_manager::UpCommand;
use crate::common::enhanced_buffer::utilities::{Command, CommandInstructions, CommandMode, CommandOrigin, CommandStatus, CommandTarget, CommandType};
use crate::common::functions::callbacks::call_callback;
use crate::common::functions::converters::convert_to_value_map;
use crate::common::structs::available_commands::{CommandPatterns, Node, NodeHandler, NodeVersion};
use crate::HOST_COMMAND_PATTERNS;
use crate::HOST_LOG_LEVEL;
use parking_lot::Mutex;
use serde_json::to_string;
use std::any::Any;
use std::boxed::Box;
use std::time::Duration;

macro_rules! acquire_logger {
    ($section_name:expr) => {{
        let host_log_level;
        {
            host_log_level = HOST_LOG_LEVEL.lock().clone();
        }
        Logger::new(host_log_level, $section_name)
    }};
}

use crate::common::structs::callbacks::{CallbackClosure, MyCallbacks};

use crate::HOST_CALLBACK_PATTERNS;

lazy_static! {
    static ref NUM_WORKERS: Arc<Mutex<u32>> = Arc::new(Mutex::new(5));
}

/// Sets the number of workers for the socket host transposer and its associated modules.
///
/// This function updates the number of worker threads that the transposer and its associated modules
/// will use for processing. This can be useful to optimize performance based on available resources.
///
/// # Parameters
///
/// - `n_workers`: The desired number of worker threads. The actual number of workers set for the
///   register manager will be 7 times this value, as each worker requires 7 threads for its operations.
///
/// # Behavior
///
/// - The register manager's workers are set to 7 times the `n_workers` value.
/// - The default number of workers is updated to `n_workers`.
/// - The number of workers for both the down buffer manager and the up buffer manager are set to `n_workers`.
///
/// # Usage
///
/// This function is typically called during the initialization phase of the socket host transposer or
/// when there's a need to adjust performance based on changing workloads or available system resources.
///
/// # Examples
///
/// ```rust
/// let desired_num_workers = 5;
/// set_socket_host_transposer_workers_num(desired_num_workers);
/// ```
///
pub fn set_socket_host_transposer_workers_num(n_workers: u32) {
    host_logger::register::register_manager::set_workers_num(n_workers.clone() * 7); // 7 * n because we need 7 for each
    let mut default_num_of_workers = NUM_WORKERS.lock();

    *default_num_of_workers = n_workers;

    enhanced_buffer::buffer_down_manager::set_workers_num(n_workers);
    enhanced_buffer::buffer_up_manager::set_workers_num(n_workers);
}

pub fn set_socket_host_transposer_callbacks(key: String, callback: CallbackClosure) {
    println!("[CLIENT][GLOBAL][Try Lock] - HOST_CALLBACK_PATTERNS");
    let patterns = &HOST_CALLBACK_PATTERNS;
    println!("[CLIENT][GLOBAL][Lock] - HOST_CALLBACK_PATTERNS");
    patterns.insert(key, callback);
    println!("[CLIENT][GLOBAL][Release] - HOST_CALLBACK_PATTERNS");
}

// > Transposer:

macro_rules! create_special_command_instruction_response {
    ($special_command:expr) => {{
        let new_command_instructions = CommandInstructions::new(
            CommandMode::Response,
            CommandType::SpecialFunction,
            CommandTarget::Origin,
            CommandStatus::Success,
            CommandOrigin::Host,
            $special_command.to_string(),
            HashMap::new(),
            "".to_string(),
        );

        new_command_instructions.to_value_map()
    }};
}

macro_rules! error_response {
    ($msg:expr) => {{
        let new_command_instructions = CommandInstructions::new(
            CommandMode::Response,
            CommandType::DirectFunction,
            CommandTarget::Origin,
            CommandStatus::Failure,
            CommandOrigin::Host,
            "error_handler".to_string(),
            HashMap::new(),
            $msg.to_string(),
        );

        new_command_instructions.to_value_map()
    }};
}

use crate::socket_host::transposer_functions::handle_direct_function::handle_direct_function;
// use crate::socket_host::transposer_functions::handle_internal_management::handle_internal_management;
use crate::socket_host::transposer_functions::handle_redirect::handle_redirect;

/// Processes a map result and generates a response based on the specified mode.
///
/// This function takes a map containing `ResultType` values, a client key, and a `DownCommand`.
/// It identifies the response mode from the map and processes the map accordingly. The function
/// supports different modes such as 'to_origin', 'redirect', and 'internal_management'. Each mode
/// dictates a different way of handling the map and preparing the response. The function also
/// handles cases where the response mode is not specified or does not match known modes.
///
/// # Arguments
/// * `m` - A HashMap with String keys and `ResultType` values representing the result to process.
/// * `client_key` - A reference to a String representing the client key.
/// * `down_command` - A reference to a `DownCommand` providing additional command context.
///
/// # Returns
/// Returns a tuple containing:
/// - `Result<String, Error>`: A `Result` object which is Ok if processing is successful, containing
///   the processed response as a JSON string, or an `Error` if processing fails.
/// - `String`: The client key, potentially modified during processing.
///
/// # Examples
/// ```
/// // Assuming HashMap, ResultType, DownCommand, and related types are defined and available
/// let mut result_map = HashMap::new();
/// result_map.insert("response_mode".to_string(), ResultType::Str("to_origin".to_string()));
/// // ... populate result_map as needed ...
///
/// let client_key = "client123".to_string();
/// let down_command = DownCommand::new(...); // Construct a DownCommand
///
/// let (response, client_key) = process_map_result(result_map, &client_key, &down_command);
/// // Handle the response and client_key as needed
/// ```
/// // TODO >>> Remake this Doc string!
pub fn process_map_result(m: &CommandInstructions, client_key: &String, parity_id: &String, priority: &u8, command_id: &Option<u32>) -> (Value, String) {
    let logger = acquire_logger!("Transposer - Process");

    let mut client_to_send: String = client_key.clone();

    let response: Value = match &m.target {
        CommandTarget::Host => {
            // -> THIS IS DESIGNED TO ALLOW HOST SEND COMMANDS TO ITSELF

            // TODO >>> IMPLEMENT SECURITY EMASURES HERE OR GIVE IT AS A USER RESPONSIBILITY

            // let resp: CommandInstructions = handle_internal_management(&m, &mut client_to_send);

            // -> HANDLE DIRECT FUNCTIONS:

            let mut result: ProcessResult = ProcessResult::Empty;

            if let Some(id) = command_id {
                result = handle_direct_function(client_key, &m.actf, m.clone(), Some(*id));

                match result {
                    ProcessResult::CommandInstructions(c) => c.to_value_map(),
                    ProcessResult::List(l) => {
                        // TODO >>> Handle this case maybe create a generalized function for all places that uses this
                        println!("Reeive a unimplemented case in process_map_result!");
                        create_special_command_instruction_response!("C210".to_string())
                    },
                    ProcessResult::Empty => {
                        create_special_command_instruction_response!("C210".to_string())
                    },
                    ProcessResult::Error(e) => {
                        logger.warn(format!("An error occurred in process_map_result. The error was: {:?}", e));
                        error_response!(format!("An error occurred in process_map_result. The error was: {:?}", e))
                    },
                }
            } else {
                logger.warn(format!("An error occurred in process_map_result. The error was: this cases require command_id to be some and not none"));
                error_response!(format!("An error occurred in process_map_result. The error was: this cases require command_id to be some and not none"))
            }
        },
        CommandTarget::Origin => m.to_value_map(),
        CommandTarget::ClientKey(key) => {
            // TODO >>> Implement the handle redirect
            let resp: CommandInstructions = handle_redirect(&m, &mut client_to_send, parity_id.clone(), priority.clone());
            resp.to_value_map()
        },
    };

    logger.debug(format!("Converted to Value: {:?}", &response));

    return (response, client_to_send);
}

/// Processes a `ResultType` command and schedules appropriate actions based on its type.
///
/// This function orchestrates the processing of various `ResultType` commands, including handling
/// maps, strings, integers, floats, booleans, lists, and other types. It leverages `process_map_result`
/// for handling `Map` types, specifically dealing with different response modes and other map-specific
/// logic. The function also manages errors, logs information, and schedules 'up' commands using an
/// enhanced buffer management system. It handles the removal of processed 'down' commands and generates
/// responses for each input type.
///
/// # Arguments
/// * `resulttype_command` - A command of type `ResultType` to be processed.
/// * `client_key` - A mutable string representing the client key.
/// * `down_command` - A `DownCommand` instance containing command-related information.
///
/// # Returns
/// This function does not return a value but orchestrates the processing of the result type command,
/// handles logging, and schedules upstream commands based on the processed results.
///
/// # Examples
/// ```
/// // Assuming ResultType, DownCommand, and related types are defined and available
/// let resulttype_command = ResultType::Map(...); // Construct a ResultType::Map
/// let mut client_key = "client123".to_string();
/// let down_command = DownCommand::new(...); // Construct a DownCommand
/// process_response_and_schedule(resulttype_command, client_key, down_command);
/// ```
// TODO >>> Remake this doc string
fn process_response_and_schedule(resulttype_command: ProcessResult, mut client_key: String, parity_id: &String, priority: &u8, command_id: u32) {
    let logger = acquire_logger!("Transposer - Process");

    let response: Value; // Errors are attached in the response and sent in the same way

    let mut client_to_send_back: String;

    // TODO >>> Change from ResultType to ProcessResult

    match resulttype_command {
        // TODO >>> Implement change of response here
        ProcessResult::CommandInstructions(m) => {
            (response, client_key) = process_map_result(&m, &client_key, parity_id, priority, &Some(command_id));
        },
        ProcessResult::List(l) => {
            let mut counter: u64 = 0;
            for res in l {
                match res {
                    ProcessResult::CommandInstructions(m) => {
                        if counter == 0 {
                            let (processed_resp, client_to_send_back) = process_map_result(&m, &client_key, parity_id, priority, &Some(command_id));
                            let up_command = UpCommand::new(&client_to_send_back, &parity_id, priority.clone(), &to_string(&processed_resp).unwrap());
                            enhanced_buffer::buffer_up_manager::buffer_up_schedule(up_command);
                        } else {
                            // -> Send to clients based in the target id
                            // -> Gen 20 digits parity id based on client
                            let special_parity_id: String = enhanced_buffer::buffer_up_manager::buffer_up_gen_valid_special_parity_id(&client_key);

                            let (processed_resp, client_to_send_back) = process_map_result(&m, &client_key, parity_id, priority, &Some(command_id));
                            let up_command = UpCommand::new(&client_to_send_back, &special_parity_id, priority.clone(), &to_string(&processed_resp).unwrap());
                            enhanced_buffer::buffer_up_manager::buffer_up_schedule(up_command);
                        }
                    },
                    _ => {
                        response = error_response!(format!("Error! Receive {:?} when expecting a Command_Instruction!", res));
                        let up_command = UpCommand::new(&client_key, &parity_id, priority.clone(), &to_string(&response).unwrap());
                        enhanced_buffer::buffer_up_manager::buffer_up_schedule(up_command);
                        break;
                    },
                }
            }
            // -> Remove the buffer down command that generated these responses since he is alwready processed
            enhanced_buffer::buffer_down_manager::buffer_down_remove_schedule_by_id(command_id.clone());
            return;
        },
        ProcessResult::Empty => response = create_special_command_instruction_response!("C210".to_string()),
        ProcessResult::Error(e) => {
            logger.warn(format!("An error occurred while converting the callback response. The error was: {:?}", e));
            response = error_response!(format!("An error occurred while converting the callback response. The error was: {:?}", e));
        },
    }

    // TODO >>> Made a better handler to the response errors

    logger.debug(format!("Function returned: {:?}", response));
    logger.info(format!("Command: {:?}, processed!", parity_id.clone()));

    let up_command = UpCommand::new(&client_key, &parity_id, priority.clone(), &to_string(&response).unwrap());

    enhanced_buffer::buffer_up_manager::buffer_up_schedule(up_command);
}

/// Processes a given `DownCommand`, executing the corresponding logic and handling redirection.
///
/// This function serves as a central processing unit for commands that come in. Based on the command's
/// contents, it can:
/// - Execute callbacks
/// - Translate commands
/// - Handle redirects
/// - Schedule `UpCommand`s for execution
///
/// # Parameters
///
/// - `py`: A Python interpreter instance, used for executing Python callbacks.
/// - `down_command`: The command to be processed.
///
/// # Flow
///
/// 1. Checks if the command is already registered. If it is, removes it from the schedule.
/// 2. Translates the `down_command` into a general `Command`.
/// 3. Retrieves the function to be executed from the command.
/// 4. Executes the callback associated with the function.
/// 5. Processes the response from the callback. This can involve:
///    - Handling direct responses
///    - Handling redirects
///    - Handling internal management commands
/// 6. Based on the processed response, schedules an `UpCommand` for execution.
///
/// # Notes
///
/// The function heavily relies on global patterns (`HOST_COMMAND_PATTERNS` and `HOST_CALLBACK_PATTERNS`)
/// which determine how commands are processed and which callbacks are executed.
///
/// The function can handle various response types including maps, strings, integers, floats, and booleans.
/// It also has error handling capabilities to handle unexpected response types or errors during processing.
///
/// # Panics
///
/// This function can panic in scenarios related to unwrapping values, especially when certain expected
/// keys are not present in command maps or when deserialization from JSON fails.
///
/// # Examples
///
/// ```rust
/// let py = Python::acquire_gil().python();
/// let down_command = DownCommand::new(...); // Initialize a DownCommand
///
/// process(py, down_command);
/// ```
///
fn process(down_command: DownCommand) {
    let logger = acquire_logger!("Transposer - Process");

    logger.debug(format!("Initializing processing!"));

    let command_is_not_registry: bool = enhanced_buffer::buffer_up_manager::check_if_parity_id_is_registered(down_command.parity_id.clone(), down_command.client_key.clone());
    let command_id: u32 = down_command.command_id.clone().unwrap();

    if !command_is_not_registry {
        logger.debug(format!("Command {}, already have a response!", down_command.parity_id.clone()));
        enhanced_buffer::buffer_down_manager::buffer_down_remove_schedule_by_id(command_id);
        return;
    }

    // TODO >>> Use the command.command or create a require type field to redirect the command to another client

    // -> One idea is to create a mandatory key in the command.command and instead of only function create a type kwarg field
    // > Type can be:
    // >    - same as origin
    // >    - redirect

    // > if it is redirect one extra kwarg is necessary that have the client_key to redirect
    // * This will create a need to have a local database in the host to store the clients
    // * and to store when is the last contact of some client, if it is some threshold value
    // * more it will remove the registered client, if it have a contact recent, this will redirect the message
    // * however if the message is becomes too old before the client the message is redirected catches it
    // * The system have to remove this old message from the buffer too.

    let translated_command: Command = match Command::from_down_command(&down_command) {
        Ok(c) => c,
        Err(_) => {
            // TODO >>> handle this erro case
            println!("Error converting COMMAND from down_command.");
            logger.warn(format!("Error converting COMMAND from down_command."));
            return;
        },
    };

    logger.debug(format!("Translated command: {:?}", translated_command));

    // TODO >>> Add a direct way to verify if it is a direct function by use the command.commandinstruction.commandtype
    let direct_functions: Vec<String> = vec!["get_registered_commands", "update_client_commands_ref", "restrictive_update_client_commands_ref", "add_client", "update_client", "remove_client"]
        .into_iter()
        .map(|s| s.to_string())
        .collect();

    let result: ProcessResult;

    if direct_functions.contains(&translated_command.command.actf.clone()) {
        // -> HANDLE DIRECT FUNCTIONS:
        result = handle_direct_function(&translated_command.client_key, &translated_command.command.actf.clone(), translated_command.command.clone(), Some(command_id));
    } else {
        // -> VERIFY IF THE COMMAND EXIST:
        {
            let mut global_command_patterns = HOST_COMMAND_PATTERNS.lock();

            // -> Remove command from schedule if it isn't on the patterns
            if !global_command_patterns.handler_exists_in("host", &translated_command.command.actf) {
                // TODO >>> Add a mecanism to check if the command exist for the target client
                // TODO >>> Also adda mecanism to commands have a target by default, and if target is host then target is host
                logger.warn(format!("Command isn't registered in the patterns"));
                enhanced_buffer::buffer_down_manager::buffer_down_remove_schedule_by_id(command_id.clone());
                logger.warn(format!("command skipped and removed from schedule"));
                return;
            }
        }

        // -> EXTRACT CALLBACK FUNCTION

        let command_instructions = translated_command.command.clone();
        let kwargs_to_call: HashMap<String, Value>;
        let mut map = HashMap::new();

        // > Get the info parameters and add to kwargs
        map.insert("mode".to_string(), serde_json::to_value(&command_instructions.mode).unwrap());
        map.insert("status".to_string(), serde_json::to_value(&command_instructions.status).unwrap());
        map.insert("origin".to_string(), serde_json::to_value(&command_instructions.origin).unwrap());
        map.insert("message".to_string(), serde_json::to_value(&command_instructions.message).unwrap());

        let mut kwargs = command_instructions.kwargs.clone();
        kwargs.insert("info".to_string(), serde_json::to_value(map).unwrap());

        let response;

        {
            // > THIS WAS DONE THIS WAY TO BE ABLE TO USE MULTITHREADING WITH HIGH INTENSIVE FUNCTION WITHOUT ANY PROBLEM
            let callback_patterns = HOST_CALLBACK_PATTERNS.clone();

            let mut args_pattern: HashMap<String, String> = HashMap::new();

            {
                let mut global_command_patterns = HOST_COMMAND_PATTERNS.lock();
                let host_node = global_command_patterns.get_node_by_key(&"host".to_string()).unwrap();
                let host_handlers = host_node.get_node_handlers().unwrap();
                let target_handler = host_handlers.get(&command_instructions.actf.clone()).unwrap();

                // Obtain the vec of handlers
                let handlers: Vec<NodeHandler> = match target_handler.as_array() {
                    Some(array) => {
                        // If the value is an array, convert it to Vec<serde_json::Value>
                        let handlers: Vec<NodeHandler> = array
                            .iter()
                            .map(|val| from_value(val.clone()).unwrap()) // Using unwrap here
                            .collect();

                        handlers
                    },
                    None => {
                        println!("The value is not an array.");
                        panic!();
                    },
                };

                // Get the target handler
                let handler = {
                    let target_handler;
                    for handler in handlers {
                        if (handler.name == command_instructions.actf) {
                            target_handler = handler.clone();
                            break;
                        }
                    }
                    target_handler
                };


                //! Find a way to convert pydic to rust dict presernving the order of the arguments to remap the 
                //! Callback arguments in the correct order, maybe will be necessary to use IndexMap to do that

                //Obtain the correct order of the kwargs
                args_pattern = handler.get_parameters();
            }

            // TODO >>> Add the node map required to organize the callback calling arguments array

            response = match callback_patterns.call(command_instructions.actf.as_str(), kwargs, args_pattern) {
                Ok(r) => r,
                Err(e) => {
                    // Existing logic to handle the error
                    logger.exception(format!("Callback error: {:?}", e));
                    let result = ProcessResult::Error(format!("{:?}", e));
                    let client_key = down_command.client_key.clone();
                    if let Some(c_id) = down_command.command_id {
                        process_response_and_schedule(result, client_key, &down_command.parity_id, &down_command.priority, c_id);
                    } else {
                        logger.warn("Can't process a command that doesn't have command id".to_string());
                        let result = ProcessResult::Error(format!("Callback with key '{}' not found!", translated_command.command.actf.clone()));
                        let client_key = down_command.client_key.clone();
                        if let Some(c_id) = down_command.command_id {
                            process_response_and_schedule(result, client_key, &down_command.parity_id, &down_command.priority, c_id);
                        } else {
                            logger.warn("Can't process a command that doesn't have command id".to_string())
                        }
                    }

                    enhanced_buffer::buffer_down_manager::buffer_down_remove_schedule_by_id(command_id.clone());

                    return;
                },
            };
        }

        // -> CALL CALLBACK FUNCTION

        // response = match call_callback(
        //     translated_command.command.actf.as_str(),
        //     translated_command.command.kwargs,
        //     callback,
        // ) {
        //     Ok(r) => r,
        //     Err(e) => {
        //         // Existing logic to handle the error
        //         logger.exception(format!("Callback error: {:?}", e));
        //         let result = ProcessResult::Error(format!("{:?}", e));
        //         let client_key = down_command.client_key.clone();
        //         if let Some(c_id) = down_command.command_id {
        //             process_response_and_schedule(
        //                 result,
        //                 client_key,
        //                 &down_command.parity_id,
        //                 &down_command.priority,
        //                 c_id,
        //             );
        //         } else {
        //             logger.warn("Can't process a command that doesn't have command id".to_string())
        //         }

        //         enhanced_buffer::buffer_down_manager::buffer_down_remove_schedule_by_id(
        //             command_id.clone(),
        //         );

        //         return;
        //     }
        // };

        // -> PROCESS CALLBACK RESPONSE

        // Assuming `result` is the Box<dyn Any> you want to check and extract the Value from
        // fn extract_json_value(result: Box<dyn Any>) -> Result<Value, String> {
        //     result
        //         .downcast::<Value>()
        //         .map(|boxed_value| *boxed_value) // Extract the Value from the Box
        //         .map_err(|_| "Returned value is not a serde_json::Value".to_string())
        // }

        // Attempt to convert it back to CommandInstructions

        // -> PROCESS CALLBACK RESPONSE:
        result = match response.downcast::<CommandInstructions>() {
            Ok(instructions_box) => {
                // Successfully downcasted, instructions_box is now a Box<CommandInstructions>
                println!("Successfully downcasted!");
                // You can now use instructions_box as Box<CommandInstructions>
                let instruction = *instructions_box;
                ProcessResult::CommandInstructions(instruction)
            },
            Err(_) => {
                // The downcast operation failed
                ProcessResult::Error("Failed to downcast callback response!".to_string())
            },
        };

        // result = match extract_json_value(response) {
        //     Ok(value) => {
        //         // let value: Value = extract_pyobject(py, r);
        //         println!("Value map extracted from callback response: {:?}", value);

        //         // Check if the Value is an object and convert it to HashMap
        //         if let Some(obj) = value.as_object() {
        //             match CommandInstructions::from_value_map(obj.clone().into_iter().collect()) {
        //                 Ok(c) => ProcessResult::CommandInstructions(c),
        //                 Err(e) => {
        //                     // TODO >>> Handle this error case
        //                     ProcessResult::Error("callback return a non valid response!".to_string())
        //                 },
        //             }
        //         } else {
        //             // TODO >>> Handle this error case
        //             ProcessResult::Error("The value is not a JSON object!".to_string())
        //         }
        //     },
        //     Err(e) => {
        //         // Handle the error or log it
        //         logger.exception(format!("Python error: {:?}", e));
        //         // You can return a default value or propagate the error further
        //         ProcessResult::Error(format!("{:?}", e))
        //     },
        // };
    }

    logger.debug(format!("Callback call response converted to rust: {:?}", result));

    let client_key = down_command.client_key.clone();

    if let Some(c_id) = down_command.command_id {
        process_response_and_schedule(result, client_key, &down_command.parity_id, &down_command.priority, c_id);
    } else {
        logger.warn("Can't process a command that doesn't have command id".to_string())
    }

    enhanced_buffer::buffer_down_manager::buffer_down_remove_schedule_by_id(command_id.clone());
}

fn clear_old_data() {
    enhanced_buffer::buffer_down_manager::buffer_down_clear_old_commands();
    enhanced_buffer::buffer_up_manager::buffer_up_clear_old_commands();
}

pub fn initialize_socket_host_transposer() {
    let logger = acquire_logger!("Transposer");

    let mut schedule: Vec<DownCommand> = enhanced_buffer::buffer_down_manager::buffer_down_list_schedule();

    if !(schedule.len() > 0) {
        // logger.debug(format!("Nothing in the schedule, skipping >>>"));
        clear_old_data();
        thread::sleep(Duration::from_millis(100));
        return;
    }

    schedule.sort_by(|a, b| b.priority.cmp(&a.priority)); // put the schedule in crescent order

    // logger.debug(format!("Schedule to process:\n{:?}\n", schedule));

    logger.info(format!("Data found in schedule!"));

    for dow_command in schedule {
        let logger = acquire_logger!("Transposer");
        logger.info(format!("get a pool worker in transposer!"));
        {
            logger.debug(format!("Start to process task!"));
            process(dow_command);
            logger.debug(format!("Finalize a process task!"));
        }
    }

    thread::sleep(Duration::from_millis(100));

    // let mut command_patterns = HOST_COMMAND_PATTERNS.lock().unwrap();

    return;

    // for stream in listener.incoming() {
    //     let stream = stream.unwrap();

    //     pool.execute(|| {
    //         handle_connection(stream);
    //     });
    // }
}
