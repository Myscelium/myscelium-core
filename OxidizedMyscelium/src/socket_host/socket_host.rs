use std::io::prelude::*;
use std::net::TcpListener;
use std::net::TcpStream;

use std::sync::{mpsc, Arc, Mutex};

use indexmap::IndexMap;
use serde_json::{from_str, Value};
use std::collections::HashMap;
use syn::Index;

use lazy_static::lazy_static;
use serde_json::json;

use crate::common::communication::decoders::read_json_from_stream;
use crate::common::enhanced_buffer;
use crate::common::enhanced_buffer::buffer_down_manager::DownCommand;
use crate::common::enhanced_buffer::buffer_up_manager::UpCommand;
use crate::common::enhanced_buffer::utilities::ResponseTarget;
use crate::common::enhanced_buffer::utilities::{Command, CommandInstructions, CommandMode, CommandOrigin, CommandStatus, CommandTarget, CommandType};
use crate::socket_host::transposer_functions::handle_redirect::handle_redirect;
use crate::ClientState;
use crate::NetworkMap;
use crate::NodeStatus;
use serde_json::to_string;

use crate::handle_manager_client_error;
use crate::socket_host::scheduler::{request_client_available_commands, send_network_available_commands};

#[macro_use]
use crate::{init_thread_pool, terminate_pool, run_in_thread_pool, wait_all_threads};
use crate::common::client_manager::manager::{check_if_client_key_exists, Client, ClientError};
use crate::common::custom_thread_pool::thread_pool::UnifiedThreadPool;

extern crate chrono;
use chrono::prelude::Utc;
use chrono::DateTime;
use chrono::Duration;

// > Global Vars Core

use crate::HOST_IS_RUNNING;
use std::sync::atomic::Ordering;

use super::functions::sync_analiser::sync_verifier;
use super::host_logger;
use super::host_logger::log_handler::Logger;
use crate::HOST_LOG_LEVEL;

use crate::common::structs::available_commands::CommandPatterns;

use crate::socket_host::sync_controller::controller::{ClientStatusPoolError, Clients};

use crate::CLIENTS_SYNC_CONTROLLER;
use crate::HOST_COMMAND_PATTERNS;

lazy_static! {
    static ref MAX_CONS: Arc<Mutex<u32>> = Arc::new(Mutex::new(5));
    static ref CLIENT_ID: Arc<Mutex<String>> = Arc::new(Mutex::new(' '.to_string()));
    static ref HEARTBEAT_CALLBACK: Arc<Mutex<std::collections::HashMap<&'static str, Box<dyn Fn() + Send + Sync + 'static>>>> = {
        let m = std::collections::HashMap::new();
        Arc::new(Mutex::new(m))
    };
    static ref CONNECTION_HANDLER_POOL: Arc<Mutex<UnifiedThreadPool>> = {
        let max_connections;
        {
            let max_conns = MAX_CONS.lock().unwrap();
            max_connections = *max_conns;
        }

        init_thread_pool!(max_connections as usize)
    };
}

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

#[macro_export]
macro_rules! handle_client_controller_error {
    ($error:expr, $client_key:expr, $logger:expr) => {
        match $error {
            ClientStatusPoolError::ClientDoesNotExist(c) => {
                $logger.warn(format!("WARNING: Client: {:?} does not exist so can't sync!", c));
            },
            ClientStatusPoolError::ClientAlreadySync(c) => {
                $logger.warn(format!("WARNING: Client: {:?} is already sync!", c));
            },
            ClientStatusPoolError::MaxSyncAttemptsReached(c) => {
                $logger.warn(format!("WARNING: Max attempts trying to sync with Client: {:?} reached!", c));
            },
            _ => {
                $logger.warn(format!("WARNING: Unexpected error trying to sync with client: {:?}!", $client_key));
            },
        }
    };
}

macro_rules! acquire_logger {
    ($section_name:expr) => {{
        let host_log_level;
        {
            host_log_level = HOST_LOG_LEVEL.lock().clone();
        }
        Logger::new(host_log_level, $section_name)
    }};
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

macro_rules! create_special_command_response {
    ($client_key:expr, $special_command:expr) => {{
        let command_instructions = CommandInstructions::new(
            CommandMode::Response,
            CommandType::SpecialFunction,
            CommandTarget::Origin,
            CommandStatus::Success,
            CommandOrigin::Host,
            $special_command.to_string(),
            HashMap::new(),
            "".to_string(),
            None, // Not required here
            None, // Not required here
            None, // Not required here
            true,
        );

        let command = Command {
            client_key: $client_key.to_string(),
            parity_id: "itisaspecialcase".to_string(),
            priority: 11,
            command: command_instructions,
        };
        command
    }};
}

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

macro_rules! handle_send_error {
    ($error:expr, $logger:expr, $client_key:expr) => {
        match $error {
            StreamError::ConnectionClosed => {
                $logger.warn(format!("[HOST][SOCKET][CLOSE CONNECTION] - {}", $client_key));
            },
            StreamError::WriteError(e) => {
                $logger.exception(format!("[HOST][SOCKET][WRITE ERROR] - {:?}", e));
                $logger.exception(format!("[HOST][SOCKET][CLOSE CONNECTION] - {}", $client_key));
            },
            StreamError::WriteSizeError(e) => {
                $logger.exception(format!("[HOST][SOCKET][WRITE SIZE ERROR] - {:?}", e));
                $logger.exception(format!("[HOST][SOCKET][CLOSE CONNECTION] - {}", $client_key));
            },
        }
    };
}

macro_rules! create_response_command {
    ($client_key:expr, $parity_id:expr, $priority:expr, $response:expr) => {{
        let command = Command {
            client_key: $client_key.to_string(),
            parity_id: $parity_id.to_string(),
            priority: $priority,
            command: $response,
        };
        command
    }};
}

/// Handles client errors by sending an appropriate error response.
///
/// # Arguments
/// * `$error` - The client error that occurred.
/// * `$stream` - The stream to send the response through.
/// * `$command` - The command related to the error.
/// * `$logger` - The logger for logging the error.
/// * `$default_message` - The default error message to use for unexpected errors.
macro_rules! handle_client_manager_error {
    ($error:expr, $stream:expr, $command:expr, $logger:expr, $default_message:expr) => {
        match $error {
            ClientError::ClientDoesNotExist(_) => {
                let message = "Your client isn't registered in the whitelist!";
                send_error_response!($stream, $command, $logger, message);
            },
            _ => {
                send_error_response!($stream, $command, $logger, $default_message);
            },
        }
    };
}
/// Sends an error response back to the client.
///
/// # Arguments
/// * `$stream` - The stream to send the response through.
/// * `$command` - The command related to the error.
/// * `$logger` - The logger for logging the error.
/// * `$message` - The error message to include in the response.
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

pub fn set_heartbeat_callback(callback_pattern: HashMap<&'static str, Box<dyn Fn() + Send + Sync + 'static>>) {
    {
        let mut heart_beat_callback = HEARTBEAT_CALLBACK.lock().unwrap();
        *heart_beat_callback = callback_pattern;
    }
}

/// Update the last contact time for a given client.
///
/// This function fetches a client based on their key and attempts to update their last contact time.
/// If the client does not exist or other errors occur, the appropriate error messages are printed.
///
/// # Parameters
/// - `client_key`: The unique key associated with the client whose last contact time needs to be updated.
pub fn update_last_contact(client_key: String) {
    let client = Client::get_by_key(&client_key);

    let logger = acquire_logger!("[Socket Host][Update Last Contact]");

    match client {
        Ok(c) => {
            logger.debug(format!("Receive client contact!"));
            handle_manager_client_error!(c.update_last_contact());
        },
        Err(e) => match e {
            ClientError::ClientAlreadyExist(e) => {
                logger.exception(format!("Error client: {} already exist", e));
            },
            ClientError::ClientDoesNotExist(e) => {
                logger.exception(format!("Error client: {} does't exist", e));
            },
            ClientError::UnexpectedError(e) => {
                logger.exception(format!("Get a unexpected error: {}", e));
            },
            _ => {
                logger.exception(format!("Get a unexpected error"));
            },
        },
    }
}

// > Socket Interactive Functions:

/// Set the maximum number of allowed connections.
///
/// This function sets the maximum number of connections and adjusts the number of worker threads accordingly.
/// Each connection requires seven workers, so the total number of workers is `7 * n_max_conns`.
///
/// # Parameters
/// - `n_max_conns`: The desired maximum number of connections.
pub fn set_max_conns(n_max_conns: u32) {
    // host_logger::register::old_register_manager::set_workers_num(n_max_conns.clone() * 7); // 7 * n because we need 7 for each
    let mut default_max_conns = MAX_CONS.lock().unwrap();
    *default_max_conns = n_max_conns;
}

use crate::common::enhanced_buffer::history::register::register::initialize_buffer_history;

/// Initializes the host buffer databases.
///
/// This function initializes the buffer databases for both up and down managers.
/// If the databases aren't already initialized, they will be created at the specified location.
///
/// # Parameters
/// - `buffer_location`: The location where the buffer databases should be initialized.
pub fn initialize_host_buffer(buffer_location: String) {
    let logger = acquire_logger!("[Socket][Initialize Host Buffer]");

    logger.info(format!("initializing the buffer database into: {}buffer.db, if not initialized!", buffer_location));

    initialize_buffer_history(&buffer_location);

    enhanced_buffer::buffer_down_manager::buffer_down_initialize_table(buffer_location.clone());
    enhanced_buffer::buffer_up_manager::buffer_up_initialize_table(buffer_location.clone());

    logger.info(format!("All buffer initialized successfully!"));

    return;
}

/// Starts and initializes the host to listen for incoming connections.
///
/// This function binds a `TcpListener` to the provided address and starts listening for incoming connections.
/// Each incoming connection is handled in a new thread from the thread pool, allowing for concurrent processing.
///
/// # Parameters
/// - `address`: The IP address and port on which the host should listen, in the format `ip:port`.
/// - `client_key`: The client ID for the host.
pub fn initialize_host(address: String, client_key: String) {
    let logger = acquire_logger!("Core");

    {
        let mut actual_client_id = CLIENT_ID.lock().unwrap();
        *actual_client_id = client_key;
    }

    let listener = TcpListener::bind(&address).unwrap();

    logger.info(format!("Listening: {}", address));

    loop {
        logger.info("Waiting conn!".to_string());

        // Keep the thread alive until HOST_IS_RUNNING is set to false
        if !HOST_IS_RUNNING.load(Ordering::SeqCst) {
            // Lock the pool and stop it
            terminate_pool!(CONNECTION_HANDLER_POOL);
            logger.info("Stopped the thread pool!".to_string());
            break;
        }

        match listener.accept() {
            Ok((mut stream, _)) => {
                // Directly run the connection handler in a new thread or a thread pool.
                // This allows the main loop to immediately go back to listening for new connections.
                run_in_thread_pool!(CONNECTION_HANDLER_POOL, {
                    // Set a read timeout of 5 seconds
                    stream.set_read_timeout(Some(std::time::Duration::new(5, 0))).unwrap();
                    handle_connection(&mut stream);
                });
            },
            Err(e) => {
                logger.warn(format!("Failed to accept a connection: {}", e));
            },
        }

        // No need to wait for all threads here. The main loop should be able to immediately proceed.
    }
}

// The incoming method is called on the listener, which returns an iterator that gives us a sequence of
// TCP streams (representing a series of connections). The server will then handle each connection in a loop.

// handle_connection is a function that handles each TCP stream. It reads from the stream into a buffer,
// then writes the contents of the buffer back to the stream.

/// Fetches all available registered command patterns.
///
/// This function retrieves and returns a clone of the global `HOST_COMMAND_PATTERNS` hashmap, which contains the registered command patterns.
///
/// # Returns
/// - A `HashMap<String, Value>` representing the cloned command patterns.
pub fn get_available_commands_registered() -> HashMap<std::string::String, IndexMap<String, String>> {
    let global_command_patterns = HOST_COMMAND_PATTERNS.lock().clone();
    return global_command_patterns.extract_all_commands().unwrap();
}

pub fn change_client_node_status_and_stream(client_key: String, new_status: NodeStatus) {
    let logger = acquire_logger!("Core");
    logger.info(format!("changed Client {} status: to: {:?}!", client_key, new_status));

    // -> Change client to offline in network map
    let mut network_map = HOST_COMMAND_PATTERNS.lock();
    let mut node = network_map.get_node_by_key(&client_key).unwrap();

    // if node.get_node_status() == new_status {
    //     logger.debug(format!("Client {} is alwready with status: {:?}!", client_key, new_status));
    //     return;
    // }

    if new_status == NodeStatus::Offline {
        let mut client_sync_manager = CLIENTS_SYNC_CONTROLLER.lock();

        logger.debug(format!("Client Sync Manager: {:?}", client_sync_manager));

        //> Reinitialize the status of the client that disconnects, so when it reconnects the
        //> First sync can occur naturally.
        client_sync_manager.get_client(&client_key).unwrap().reinitialize();
    }

    // -> Make all the client related to this client need to sync again by change this node status to Offline
    node.change_node_status(new_status);
}

pub fn handle_client_disconnect(client_key: &String) {
    change_client_node_status_and_stream(client_key.clone(), NodeStatus::Offline);

    // > Verify the nodes that needs to be notified of this update (restrictivety without cause waves of unecessary updates)
    sync_verifier();
}

// > Socket main structure:

/// Handles special command functions based on their string representation.
///
/// This function checks the provided function string and returns an appropriate `Command` based on predefined special cases.
/// Special cases currently supported are "C202" (Connection conf request) and "C206" (Ping request).
///
/// # Parameters
/// - `client_key`: The client ID associated with the request.
/// - `function`: The string representation of the special function to be handled.
///
/// # Returns
/// - A `Command` object representing the response for the special function.
fn handle_special_functions(client_key: String, function: String) -> Command {
    let command;
    let logger = acquire_logger!("Core");

    if function == "C202" {
        // -> Connection conf request
        command = create_special_command_response!(client_key, "C200");
    } else if function == "C206" {
        // -> Ping request

        let up_schedule: Vec<UpCommand> = enhanced_buffer::buffer_up_manager::buffer_up_list_schedule_fo_client_id(client_key.clone());

        if !(up_schedule.len() > 0) {
            return create_special_command_response!(client_key, "C207"); // If don't have any response to send send C207 that is a ping confirmation
        }

        let command_response = &up_schedule[0];

        let response_command = match Command::from_up_command(&command_response) {
            Ok(c) => c,
            Err(e) => {
                // TODO >>> Handle the invalid Commands cases
                logger.debug(format!("Command received during ping: {} is invalid, gives error: {:?}! Returning C207", command_response, e));
                return create_special_command_response!(client_key, "C207");
            },
        };

        enhanced_buffer::buffer_up_manager::buffer_up_remove_schedule_by_parity_id(&client_key, &response_command.parity_id);

        return response_command;
    } else {
        // -> Receive conf
        command = create_special_command_response!(client_key, "C210");
    }

    return command;
}

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

/// Enum representing possible responses.
///
/// This enum encapsulates the two possible response types:
/// 1. A valid `Command` response.
/// 2. An absence of a response, represented by `None`.
enum Response {
    Command(Command),
    None,
}

/// Retrieves a response for a given command.
///
/// This function is responsible for fetching a pre-scheduled response based on the provided command's client ID and parity ID.
/// Once found, the scheduled response is transformed into a `Response::Command` variant, and the original schedule is removed from the buffer.
///
/// # Parameters
/// - `command`: The `Command` object for which a response needs to be fetched.
///
/// # Returns
/// - `Response::Command(response_command)`: If a pre-scheduled response is found in the buffer.
/// - `Response::None`: If no scheduled response is found for the provided command.
///
/// # Logic Flow
/// 1. The function queries the `buffer_up_manager` to fetch any scheduled responses that match the provided command's client ID and parity ID.
/// 2. If no scheduled response is found, the function returns a `Response::None`.
/// 3. If a scheduled response is found, it is converted into a `Command` object.
/// 4. The original scheduled response is then removed from the buffer to avoid any future retrievals.
/// 5. The transformed command is returned as `Response::Command(response_command)`.
fn get_response(command: Command) -> Response {
    let up_schedule: Vec<UpCommand> = enhanced_buffer::buffer_up_manager::buffer_up_get_scheduled_by_parity_id(&command.client_key, &command.parity_id);

    if !(up_schedule.len() > 0) {
        return Response::None;
    }

    let command_response = &up_schedule[0];
    let command_response_command = serde_json::from_str(command_response.command.as_str()).unwrap();
    let response_command = create_response_command!(command_response.client_key, command_response.parity_id, command_response.priority, command_response_command);
    enhanced_buffer::buffer_up_manager::buffer_up_remove_schedule_by_parity_id(&command.client_key, &response_command.parity_id);
    return Response::Command(response_command);
}

const MAX_DATA_SIZE: usize = 10 * 1024 * 1024; // For example, 10 MB

#[derive(Debug)]
enum StreamError {
    WriteError(std::io::Error),
    WriteSizeError(std::io::Error),
    ConnectionClosed,
}

fn send(stream: &mut TcpStream, data: Command) -> Result<(), StreamError> {
    let command_response_json = json!(data).to_string();
    let data_size = command_response_json.len() as u32;
    let size_buffer = data_size.to_be_bytes();

    // Check if the connection was closed
    if stream.peer_addr().is_err() {
        return Err(StreamError::ConnectionClosed);
    }

    // Send the size of the data
    match stream.write(&size_buffer) {
        Ok(_) => {},
        Err(e) => {
            return Err(StreamError::WriteSizeError(e));
        },
    };

    // Send the actual data
    match stream.write(command_response_json.as_bytes()) {
        Ok(_) => {},
        Err(e) => {
            return Err(StreamError::WriteError(e));
        },
    };

    Ok(())
}

/// This function updates the node sync status attempt
/// > Important! - This function require globals:
/// - HOST_COMMAND_PATTERNS
/// - CLIENT_SYNC_CONTROLLER
/// Make sure to have them free before try to call this function to avoid blockages
fn update_client_sync_attempt(client_key: &String, logger: &Logger) -> bool {
    let mut controller = CLIENTS_SYNC_CONTROLLER.lock();

    let client = controller.get_client(client_key).unwrap();

    {
        let mut actual_patterns = HOST_COMMAND_PATTERNS.lock();

        let mut ref_node = actual_patterns.get_node_by_key(client_key).unwrap();

        // > If the sync is halth of the attempts and not sync yet, change the client status to NotSyncYet
        if client.get_sync_attempts() >= (client.get_max_sync_attempts() / 2) {
            ref_node.change_node_status(NodeStatus::NotSyncYet)
        }

        // -> If is the first time that the node is connecting, change it's status to not sync, this is important
        // -> to dependent clies know that it is in sync process, and since it will only happen the first time,
        // -> it will not trigger massive loops of sync because it don't change other nodes status to NotSyncYet, only this
        // -> node iteself and only in the first time that it is tring to sync, this will help other nodes awaits,
        // -> know that the node is initializing and this will make them wait to give an exception or send someting to this one.

        if ref_node.get_node_status() == NodeStatus::NotImplemented || ref_node.get_node_status() == NodeStatus::Offline {
            ref_node.change_node_status(NodeStatus::NotSyncYet)
        }
    }

    // > This function auto handles the error of max attempt reached too
    if let Err(e) = controller.update_client_sync_attempt(client_key) {
        handle_client_controller_error!(e.clone(), client_key, logger); // This only logs the error
        match e {
            ClientStatusPoolError::ClientAlreadyExist(_) => unreachable!(),
            ClientStatusPoolError::ClientDoesNotExist(_) => unreachable!(),
            ClientStatusPoolError::MaxSyncAttemptsReached(_) => {
                handle_client_disconnect(&client_key); // Disconnect the client, what should trigger sync in all dependent ones
                return true;
            },
            ClientStatusPoolError::ClientAlreadySync(_) => change_client_node_status_and_stream(client_key.clone(), NodeStatus::Online),
        }
    }

    return false;
}

/// Handles an individual connection to the server.
///
/// This function is responsible for managing the lifecycle of a client connection to the server.
/// Upon receiving data from the client, it processes the received commands, logs relevant information,
/// and sends back appropriate responses.
///
/// # Parameters
/// - `stream`: The TCP stream associated with the client connection.
///
/// # Flow
/// 1. A logger specific to the "Core" section is acquired for logging purposes.
/// 2. The function continuously reads data from the client's stream.
/// 3. If no data is read, it simply continues to the next iteration.
/// 4. Upon successfully reading data, the buffer is converted to a string and then deserialized into a `Command` object.
/// 5. The function then checks if the received command is a "special function" or a recognized regular command.
/// 6. If the command is neither special nor recognized, an error response is sent back to the client.
/// 7. Throughout the process, all key events and errors are logged.
///
/// # Notes
/// - The TODO within the function indicates a need to enhance error handling during deserialization of the command.
/// - Special care is given to the handling of special functions, which are identified by specific codes (e.g., "C202" and "C206").
/// - There is a mechanism in place to check if a command's parity ID is already registered and to retrieve existing responses if necessary.
fn handle_connection(stream: &mut TcpStream) {
    // Aquire logger to section Handle Conn
    let logger = acquire_logger!("Core");
    let mut client_key: String = "".to_string();

    // -> Before join in the loop, schedule a request of the client commands
    let mut client: Option<Client> = None;

    loop {
        let mut size_buffer = [0; 4];

        //> Read the size of the incoming data
        let data_size = match stream.read_exact(&mut size_buffer) {
            Ok(_) => u32::from_be_bytes(size_buffer) as usize,
            Err(e) => {
                logger.debug(format!("Failed to read from the stream: {:?}", e));
                eprintln!("Failed to read from the stream: {:?}", e);
                //> Handle the error, e.g., by returning from the function or taking corrective action
                handle_client_disconnect(&client_key);
                break; //> or handle differently
            },
        };

        if data_size > MAX_DATA_SIZE {
            logger.exception(format!("Data size too large: {}", data_size));
            break; //> Close connection or handle appropriately
        }

        logger.debug(format!("Receiving data with length: {}", data_size));

        //> Allocate a buffer of the appropriate size
        let mut data_buffer = vec![0; data_size];

        //> Read the data into the buffer
        let buffer_string = match stream.read_exact(&mut data_buffer) {
            Ok(_) => String::from_utf8_lossy(&data_buffer).trim_end_matches(|c| c == '\n' || c == '\r' || c == '\0').to_string(),
            Err(e) => {
                logger.debug(format!("Failed to read from the stream: {:?}", e));
                eprintln!("Failed to read from the stream: {:?}", e);
                //> Handle the error, e.g., by returning from the function or taking corrective action
                handle_client_disconnect(&client_key);
                break; //> or handle differently
            },
        };

        let command: Command = serde_json::from_str(&buffer_string).unwrap();
        logger.debug(format!("Command received:\n{:?}\n", command));
        let special_functions: Vec<String> = vec!["C202".to_string(), "C206".to_string()];

        if !check_if_client_key_exists(command.client_key.clone()) {
            // -> In case client isn't registered in the clients allowed

            let response: Command = create_error_command_response!(command.client_key, command.parity_id, "Your client isn't registered in the whitelist!");
            logger.exception(format!("WARNING: Client isn't registered, sending back: {:?}", response));

            match send(stream, response) {
                Ok(_) => {},
                Err(e) => {
                    handle_send_error!(e, logger, command.client_key);
                    break;
                },
            };

            break;
        }

        client_key = command.client_key.clone();

        client = Some(match Client::get_by_key(&command.client_key) {
            Ok(c) => c,
            Err(e) => {
                handle_client_manager_error!(e, stream, command, logger, "Unexpected error getting your client");
                break;
            },
        });

        // -> GET CLIENT STATUS, SEE IF IT IS SYNC OR NOT
        let client_sync_status: Option<bool>;
        let client_last_sync: Option<DateTime<Utc>>;

        {
            let mut controller = CLIENTS_SYNC_CONTROLLER.lock();
            client_sync_status = match controller.get_sync_status(&command.client_key.clone()) {
                Ok(s) => Some(s),
                Err(e) => {
                    handle_client_controller_error!(e, &command.client_key, logger);
                    None
                },
            };
            client_last_sync = match controller.get_last_sync(&command.client_key.clone()) {
                Ok(last_sync) => Some(last_sync),
                Err(e) => {
                    handle_client_controller_error!(e, &command.client_key, logger);
                    None
                },
            };
            logger.debug(format!("Clients In Sync Controller: {:?}", controller));
        }

        update_last_contact(command.client_key.clone());

        // > Check if the max sync was reached
        // > if is first sync and yes, diconnect client
        // > if is not first sync and yes,change client status to not sync
        // > This should auto trigger sync to all clients that isn't sync in relation to the network map available for them

        // -> Refactored SYNC CONTROLLER:
        if let Some(sync) = client_sync_status {
            if !sync {
                logger.debug(format!("\nClient: {:?} isn't sync\n", &command.client_key));

                let current_time = Utc::now();
                let should_attempt_sync = client_last_sync.map_or(true, |last_sync| current_time - last_sync > Duration::seconds(30));

                if should_attempt_sync {
                    logger.info(format!("Try to sync with: {}", command.client_key));
                    send_network_available_commands(command.client_key.clone());
                    if update_client_sync_attempt(&command.client_key, &logger) {
                        break;
                    };
                    //> The new system only stream that the node connect here and is trying to sync so this new
                    //> node is with NotSyncYet status.
                    //> Then wen this node connects we change the status to Sync. If node isn't able to sync, we
                    //> change it to offline and disconnect it. Also another thing that we can do is impl a new Idle status that can be
                    //> represented as a pulsating orange color.
                } else if let Some(last_sync) = client_last_sync {
                    logger.info(format!(
                        "WARNING: Client: {:?} not sync yet, trying again in: {:?} seconds!",
                        &command.client_key,
                        (Duration::seconds(30) - (current_time - last_sync)).num_seconds()
                    ));
                }
            } else {
                logger.debug(format!("\nClient: {:?} is sync!\n", &command.client_key));
            }
        } else {
            break;
        }

        // ! WE CAN'T USE THIS PY AQUIRE UNTIL THE PYTHON POOL IS FINISHED !

        // -> ---------------------------------------------------------------------------------------------------------------------
        // -> HOST FUNCTION VERIFICATION
        {
            let mut command_patterns;

            {
                command_patterns = HOST_COMMAND_PATTERNS.lock().clone();
            }

            logger.debug(format!("[HOST][REGIRSTRED PATTERNS]:\n{:?}", command_patterns));

            logger.debug(format!("\nCommand.Command: {:?}", command.command));
            logger.debug(format!("\nCommand.Command.function: {:?}", command.command.actf));
            logger.debug(format!("Command function: {}", command.command.actf));

            let direct_functions: Vec<String> = vec!["get_registered_commands", "update_client_commands_ref", "restrictive_update_client_commands_ref", "add_client", "update_client", "remove_client"]
                .into_iter()
                .map(|s| s.to_string())
                .collect();

            match &command.command_type() {
                CommandType::SpecialFunction => {
                    // -> HANDLE SPECIAL FUNCTION CASES:
                    if special_functions.contains(&command.command.actf) {
                        let response: Command = handle_special_functions(command.client_key.clone(), command.command.actf.clone());
                        logger.debug(format!("Sending back: {:?}", response));

                        match send(stream, response) {
                            Ok(_) => {},
                            Err(e) => handle_send_error!(e, logger, command.client_key),
                        };
                    }
                },
                _ => {
                    // -> HANDLE HOST FUNCTIONS - DIRECT AND EXTERNAL FUNCTION:

                    match &command.command.target {
                        CommandTarget::ClientKey(target) => {
                            // TODO >>> WHEN ADD THE PERMISSIONS ADD A MECHANISM TO CHECK IF THE CLIENT HAS PERMISSION TO ACCESS THIS ENDPOINTS

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
                                match send(stream, command) {
                                    Ok(_) => {},
                                    Err(e) => handle_send_error!(e, logger, client_key),
                                };
                                handle_client_disconnect(&client_key);
                                break;
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
                                match send(stream, command) {
                                    Ok(_) => {},
                                    Err(e) => handle_send_error!(e, logger, client_key),
                                };
                                handle_client_disconnect(&client_key);
                                break;
                            }

                            //> SEE IF THE HANDLER EXIST IN THE TARGET

                            if command.command.mode == "Function" {
                                if !command_patterns.handler_exists_in(target.as_str(), command.command.actf.as_str()) {
                                    let command: Command = create_error_command_response!(command.client_key.clone(), command.parity_id, format!("Function: {}, Doesn't exist in target client: {}!", command.command.actf, target));
                                    logger.debug(format!("Sending back: {:?}", &command));
                                    let client_key = command.client_key.clone();
                                    match send(stream, command) {
                                        Ok(_) => {},
                                        Err(e) => handle_send_error!(e, logger, client_key),
                                    };
                                    handle_client_disconnect(&client_key);
                                    break;
                                };
                            }

                            // TODO >>> See if the client has permission to send commands to this target

                            //>--------------------------------------------------------------------------------------------------
                            //> Response Target Rules

                            //* Command Target should't be the same of the Response Target
                            //* When a Client Sends a command the scheduler verify if the handler exists in itself
                            //* Same happens for host

                            // TODO >>> Add verification to cases where client is sendind a resp to host, verify if the resp_actf exists in target

                            // TODO >>> ADD THIS TO THE OTHER CASES< NOT ONLY TO THE CommandTarget::ClientKey
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
                                    match send(stream, command) {
                                        Ok(_) => {},
                                        Err(e) => handle_send_error!(e, logger, client_key),
                                    };
                                    handle_client_disconnect(&client_key);
                                    break;
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
                                        match send(stream, command) {
                                            Ok(_) => {},
                                            Err(e) => handle_send_error!(e, logger, client_key),
                                        };
                                        handle_client_disconnect(&client_key);
                                        break;
                                    }

                                    //> Check if the handler to response exist in target (ONLY IF AUTO COLLECT == True)
                                    if command.command.collect_response {
                                        if let Some(response_actf) = command.command.response_actf.clone() {
                                            if command.command.collect_response && response_actf != "" {
                                                // Only verify if handler exists if auto collect response == true
                                                if !command_patterns.handler_exists_in(resp_target.as_str(), response_actf.as_str()) {
                                                    let command: Command =
                                                        create_error_command_response!(command.client_key.clone(), command.parity_id, format!("Response Handler: {}, Doesn't exist in target client: {}!", command.command.actf, target));
                                                    logger.debug(format!("Sending back: {:?}", &command));
                                                    let client_key = command.client_key.clone();
                                                    match send(stream, command) {
                                                        Ok(_) => {},
                                                        Err(e) => handle_send_error!(e, logger, client_key),
                                                    };
                                                    handle_client_disconnect(&client_key);
                                                    break;
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
                            let response: Command;

                            //> HANDLE COMMANDS WITH RESPONSE:
                            if !command_is_not_registry {
                                logger.warn(format!("Command {}, already have a response!", command.parity_id.clone()));
                                match get_response(command.clone()) {
                                    Response::Command(c) => {
                                        if c.client_key == command.client_key {
                                            response = c;
                                        } else {
                                            logger.info("Response is None!".to_string());
                                            response = create_special_command_confirmation!(command.client_key.clone(), command.parity_id.clone());
                                        }
                                    },
                                    Response::None => {
                                        logger.info("Response is None!".to_string());
                                        response = create_special_command_confirmation!(command.client_key.clone(), command.parity_id.clone());
                                    },
                                }

                            //> HANDLE COMMANDS WITHOUT RESPONSES:
                            } else {
                                // _ = handle_common_function(&command_to_redirect);
                                let up_command = UpCommand::from_command(command_to_redirect.clone());
                                enhanced_buffer::buffer_up_manager::buffer_up_schedule(up_command);
                                response = create_special_command_confirmation!(command.client_key.clone(), command.parity_id.clone());
                            }

                            //> SEND RESPONSE BACK - HERE IT CAN BE COMMAND RESPONSES OR CONFIRMATIONS
                            logger.debug(format!("Sending back: {:?}", response));
                            match send(stream, response) {
                                Ok(_) => {},
                                Err(e) => {
                                    handle_send_error!(e, logger, command_to_redirect.client_key)
                                },
                            };
                        },
                        CommandTarget::Host => {
                            //> CHECK IF HANDLER DON'T EXIST AND RETURN & SEND ERROR MESSAGE IF NOT
                            if !command_patterns.handler_exists_in("host", command.command.actf.as_str()) && !direct_functions.contains(&command.command.actf) {
                                let command: Command = create_error_command_response!(command.client_key.clone(), command.parity_id, format!("Function: {}, Doesn't exist in host callbacks nor in any client!", command.command.actf));
                                logger.debug(format!("Sending back: {:?}", &command));
                                let client_key = command.client_key.clone();
                                match send(stream, command) {
                                    Ok(_) => {},
                                    Err(e) => handle_send_error!(e, logger, client_key),
                                };
                                handle_client_disconnect(&client_key);
                                break;
                            };

                            // > VERIFY IF ALREADY PROCESSED:
                            logger.debug("Command is in command patterns!".to_string());
                            let command_is_not_registry: bool = enhanced_buffer::buffer_up_manager::check_if_parity_id_is_registered(command.parity_id.clone(), command.client_key.clone());
                            let response: Command;

                            //> HANDLE COMMANDS WITH RESPONSE:
                            if !command_is_not_registry {
                                logger.warn(format!("Command {}, already have a response!", command.parity_id.clone()));
                                match get_response(command.clone()) {
                                    Response::Command(c) => {
                                        if c.client_key == command.client_key {
                                            response = c;
                                        } else {
                                            logger.info("Response is None!".to_string());
                                            response = create_special_command_confirmation!(command.client_key.clone(), command.parity_id.clone());
                                        }
                                    },
                                    Response::None => {
                                        logger.info("Response is None!".to_string());
                                        response = create_special_command_confirmation!(command.client_key.clone(), command.parity_id.clone());
                                    },
                                }

                            //> HANDLE COMMANDS WITHOUT RESPONSES:
                            } else {
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
                                        match send(stream, command) {
                                            Ok(_) => {},
                                            Err(e) => handle_send_error!(e, logger, client_key),
                                        };
                                        handle_client_disconnect(&client_key);
                                        break;
                                    }

                                    //> If resp target isn't origin, nor host then:
                                    if !vec!["origin", "host"].contains(&resp_target.as_str()) {
                                        let available_targets_map = command_patterns.get_node_keys().unwrap();
                                        let available_targets_keys: Vec<String> = available_targets_map.into_iter().map(|(_, value)| value).collect();

                                        //> CHECK IF THE TARGET EXISTS
                                        if !available_targets_keys.contains(&resp_target) {
                                            let command: Command = create_error_command_response!(command.client_key.clone(), command.parity_id, format!("Response target: {} isn't reachable", &resp_target.as_str()));
                                            logger.debug(format!("Sending back: {:?}", &command));
                                            let client_key = command.client_key.clone();
                                            match send(stream, command) {
                                                Ok(_) => {},
                                                Err(e) => handle_send_error!(e, logger, client_key),
                                            };
                                            handle_client_disconnect(&client_key);
                                            break;
                                        }

                                        //> Check if the target is ready
                                        // TODO >>> Possible waith to target become ready
                                        if !command_patterns.target_is_ready(&resp_target).unwrap() {
                                            let command: Command = create_error_command_response!(command.client_key.clone(), command.parity_id, format!("Response target: {} isn't ready yet", &resp_target.as_str()));
                                            logger.debug(format!("Sending back: {:?}", &command));
                                            let client_key = command.client_key.clone();
                                            match send(stream, command) {
                                                Ok(_) => {},
                                                Err(e) => handle_send_error!(e, logger, client_key),
                                            };
                                            handle_client_disconnect(&client_key);
                                            break;
                                        }

                                        //> Check if the handler to response exist in target (this also will handler the case that the target isn't initialized)
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
                                                    let client_key = command.client_key.clone();
                                                    match send(stream, command) {
                                                        Ok(_) => {},
                                                        Err(e) => handle_send_error!(e, logger, client_key),
                                                    };
                                                    handle_client_disconnect(&client_key);
                                                    break;
                                                };
                                            }
                                        }
                                    }
                                };

                                response = handle_common_function(&command);
                            }

                            //> SEND RESPONSE BACK - HERE IT CAN BE COMMAND RESPONSES OR CONFIRMATIONS
                            logger.debug(format!("Sending back: {:?}", response));
                            match send(stream, response) {
                                Ok(_) => {},
                                Err(e) => handle_send_error!(e, logger, command.client_key),
                            };
                        },
                        _ => {
                            // -> HANDLE THE CASE WERE A COMMAND DOES EXISTS HERE IN HOST NOR IN ANY NODE THAT CLIENT HAS PERMISSION
                            let command: Command = create_error_command_response!(
                                command.client_key.clone(),
                                command.parity_id,
                                format!("Command: {:?}, isn't valid, you cant send a command to host with a target origin, this isn't allowed!", command.command)
                            );
                            logger.debug(format!("Sending back: {:?}", &command));
                            let client_key = command.client_key.clone();
                            match send(stream, command) {
                                Ok(_) => {},
                                Err(e) => handle_send_error!(e, logger, client_key),
                            };
                            handle_client_disconnect(&client_key);
                            break;
                        },
                    }
                },
            }
        }
    }

    handle_client_disconnect(&client_key);
}
