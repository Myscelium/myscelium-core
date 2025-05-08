use std::io::prelude::*;

use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    io::{Error, ErrorKind},
    sync::Arc,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{
        mpsc::{self, Sender},
        Mutex, OnceCell,
    },
};

use std::thread;

use indexmap::IndexMap;
use serde_json::{from_str, Value};
use syn::Index;

use crate::common::{enhanced_buffer::utilities::CommandVariant, types::BufferError};
use crate::socket_host::command_handler::{host_commands_processing, redirect_commands_processing};
use crate::socket_host::task_manager::manager::NodeTask;
use crate::TASKS_MANAGER;
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

/// Dispatcher Units: for example, Transposer is a "worker" that processes certain messages.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum Unit {
    Transposer,
}

/// Type aliases for clarity.
type ClientMap = Arc<Mutex<HashMap<String, mpsc::Sender<String>>>>;
type UnitSenders = Arc<HashMap<Unit, mpsc::Sender<(String, String)>>>;

lazy_static! {
    static ref MAX_CONS: Lazy<Arc<Mutex<u32>>> = Lazy::new(|| Arc::new(Mutex::new(5)));
    static ref CLIENT_ID: Arc<Mutex<String>> = Arc::new(Mutex::new(' '.to_string()));
    static ref HEARTBEAT_CALLBACK: Arc<Mutex<std::collections::HashMap<&'static str, Box<dyn Fn() + Send + Sync + 'static>>>> = {
        let m = std::collections::HashMap::new();
        Arc::new(Mutex::new(m))
    };
    pub static ref CONNECTION_HANDLER_POOL: OnceCell<Arc<std::sync::Mutex<UnifiedThreadPool>>> = OnceCell::const_new();
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
                $logger.warn(format!("WARNING: Client: {:?} does not exist so can't sync!", c)).await;
            },
            ClientStatusPoolError::ClientAlreadySync(c) => {
                $logger.warn(format!("WARNING: Client: {:?} is already sync!", c)).await;
            },
            ClientStatusPoolError::MaxSyncAttemptsReached(c) => {
                $logger.warn(format!("WARNING: Max attempts trying to sync with Client: {:?} reached!", c)).await;
            },
            _ => {
                $logger.warn(format!("WARNING: Unexpected error trying to sync with client: {:?}!", $client_key)).await;
            },
        }
    };
}

macro_rules! acquire_logger {
    ($section_name:expr) => {{
        let host_log_level;
        {
            host_log_level = HOST_LOG_LEVEL.lock().await.clone();
        }
        Logger::new(host_log_level, $section_name).await
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
                $logger.warn(format!("[HOST][SOCKET][CLOSE CONNECTION] - {}", $client_key)).await;
            },
            StreamError::WriteError(e) => {
                $logger.exception(format!("[HOST][SOCKET][WRITE ERROR] - {:?}", e)).await;
                $logger.exception(format!("[HOST][SOCKET][CLOSE CONNECTION] - {}", $client_key)).await;
            },
            StreamError::WriteSizeError(e) => {
                $logger.exception(format!("[HOST][SOCKET][WRITE SIZE ERROR] - {:?}", e)).await;
                $logger.exception(format!("[HOST][SOCKET][CLOSE CONNECTION] - {}", $client_key)).await;
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
        $logger.exception(format!("WARNING: {}, sending back: {:?}", $message, response)).await;
        match send($stream, response) {
            Ok(_) => {},
            Err(e) => {
                handle_send_error!(e, $logger, $command.client_key);
                break;
            },
        }
    };
}

pub async fn set_heartbeat_callback(callback_pattern: HashMap<&'static str, Box<dyn Fn() + Send + Sync + 'static>>) {
    {
        let mut heart_beat_callback = HEARTBEAT_CALLBACK.lock().await;
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
pub async fn update_last_contact(client_key: String) -> Result<(), ClientError> {
    let client = Client::get_by_key(&client_key).await;

    let logger = acquire_logger!("[Socket Host][Update Last Contact]");

    match client {
        Ok(c) => {
            logger.debug(format!("Receive client contact!")).await;
            c.update_last_contact();
        },
        Err(e) => return Err(e),
        // ClientError::ClientAlreadyExist(e) => {
        //     logger.exception(format!("Error client: {} already exist", e));
        // },
        // ClientError::ClientDoesNotExist(e) => {
        //     logger.exception(format!("Error client: {} does't exist", e));
        // },
        // ClientError::UnexpectedError(e) => {
        //     logger.exception(format!("Get a unexpected error: {}", e));
        // },
        // _ => {
        //     logger.exception(format!("Get a unexpected error"));
        // },
    }

    Ok(())
}

// > Socket Interactive Functions:

/// Set the maximum number of allowed connections.
///
/// This function sets the maximum number of connections and adjusts the number of worker threads accordingly.
/// Each connection requires seven workers, so the total number of workers is `7 * n_max_conns`.
///
/// # Parameters
/// - `n_max_conns`: The desired maximum number of connections.
pub async fn set_max_conns(n_max_conns: u32) {
    // host_logger::register::old_register_manager::set_workers_num(n_max_conns.clone() * 7); // 7 * n because we need 7 for each
    let mut default_max_conns = MAX_CONS.lock().await;
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
pub async fn initialize_host_buffer(buffer_location: String) {
    let logger = acquire_logger!("[Socket][Initialize Host Buffer]");

    logger.info(format!("initializing the buffer database into: {}buffer.db, if not initialized!", buffer_location)).await;

    initialize_buffer_history(&buffer_location); // TODO >>> This should be async?
    enhanced_buffer::buffer_down_manager::buffer_down_initialize_table(buffer_location.clone()).await;
    enhanced_buffer::buffer_up_manager::buffer_up_initialize_table(buffer_location.clone()).await;

    logger.info(format!("All buffer initialized successfully!")).await;

    return;
}

use std::panic;

// Call this during app startup
pub async fn initialize_connection_pool() {
    // Lock the MAX_CONS mutex and extract the value
    let max_conns = MAX_CONS.lock().await;
    let max_connections = *max_conns;

    // Initialize your thread pool
    let pool = init_thread_pool!(max_connections as usize);

    // Wrap it in Arc<Mutex<...>> and store it globally
    CONNECTION_HANDLER_POOL.set(pool).expect("CONNECTION_HANDLER_POOL already set");
}

pub async fn initialize_host(address: String, client_key: String) -> std::io::Result<()> {
    initialize_connection_pool();
    let logger = acquire_logger!("Core");

    {
        let mut actual_client_id = CLIENT_ID.lock().await;
        *actual_client_id = client_key;
    }

    let listener = TcpListener::bind(&address).await?;

    logger.info(format!("Listening: {}", address)).await;

    // Shared map from client_id -> Sender, so we can reply to each client.
    let client_txs: ClientMap = Arc::new(Mutex::new(HashMap::new()));

    // Create a channel for the Transposer unit, spawn its async task
    let (tx_transposer, rx_transposer) = mpsc::channel::<(String, String)>(32);
    {
        // Clone for the transposer background task
        // let client_txs_clone = Arc::clone(&client_txs);
        // tokio::spawn(async move {
        //     transposer(rx_transposer, client_txs_clone).await;
        // });
    }

    // Put the transposer channel sender into a global map, in case we have more units later.
    let mut senders = HashMap::new();
    senders.insert(Unit::Transposer, tx_transposer);
    let unit_senders = Arc::new(senders);

    loop {
        let logger = acquire_logger!("Core");
        logger.info("Waiting conn!".to_string()).await;

        // Keep the thread alive until HOST_IS_RUNNING is set to false
        if !HOST_IS_RUNNING.load(Ordering::SeqCst) {
            logger.info("Stopped the server!".to_string()).await;
            break;
        }

        let (stream, addr) = listener.accept().await?;
        println!("Accepted connection from {:?}", addr);

        let unit_senders_clone: Arc<HashMap<Unit, Sender<(String, String)>>> = Arc::clone(&unit_senders);
        let client_txs_clone: Arc<Mutex<HashMap<String, Sender<String>>>> = Arc::clone(&client_txs);

        // Spawn a new task per connection
        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, unit_senders_clone, client_txs_clone).await {
                logger.exception(format!("Connection handler panicked: {:?}", e)).await;
            }
        });
    }

    return Ok(());
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
pub async fn get_available_commands_registered() -> HashMap<std::string::String, IndexMap<String, String>> {
    let global_command_patterns = HOST_COMMAND_PATTERNS.lock().await.clone();
    return global_command_patterns.extract_all_commands().unwrap();
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ChangeStatusError {
    NodeDoNotExists(String),
    IncorrectValueMapPattern(String),
    IncorrectValuePattern,
    NodeNotInitialized(String),
    ClientAlreadyExist(String),
    ClientDoesNotExist(String),
    MaxSyncAttemptsReached(String),
    ClientAlreadySync(String),
}

pub async fn change_client_node_status_and_stream(client_key: String, new_status: NodeStatus) -> Result<(), ChangeStatusError> {
    let logger = acquire_logger!("Core");
    logger.info(format!("changed Client {} status: to: {:?}!", client_key, new_status)).await;

    // -> Change client to offline in network map
    let mut network_map = HOST_COMMAND_PATTERNS.lock().await;
    let mut node = network_map.get_node_by_key(&client_key).map_err::<ChangeStatusError, _>(Into::into)?;

    // if node.get_node_status() == new_status {
    //     logger.debug(format!("Client {} is alwready with status: {:?}!", client_key, new_status));
    //     return;
    // }

    if new_status == NodeStatus::Offline {
        let mut client_sync_manager = CLIENTS_SYNC_CONTROLLER.lock().await;

        logger.debug(format!("Client Sync Manager: {:?}", client_sync_manager)).await;

        //> Reinitialize the status of the client that disconnects, so when it reconnects the
        //> First sync can occur naturally.
        let csm = client_sync_manager.get_client(&client_key).map_err::<ChangeStatusError, _>(Into::into)?;
        csm.reinitialize();
    }

    // -> Make all the client related to this client need to sync again by change this node status to Offline
    node.change_node_status(new_status);

    return Ok(());
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
async fn handle_special_functions(client_key: String, function: String) -> Command {
    let command;
    let logger = acquire_logger!("Core");

    if function == "C202" {
        // -> Connection conf request
        command = create_special_command_response!(client_key, "C200");
        println!("Received connection conf request C202, casting C200!");
    } else if function == "C206" {
        // -> Ping request

        let up_schedule: Vec<UpCommand> = match enhanced_buffer::buffer_up_manager::buffer_up_list_schedule_fo_client_id(client_key.clone()).await {
            Ok(ups) => ups,
            Err(e) => {
                logger.exception(format!("Error trying to load up_scheduler buffer for the client: {:?}, Error: {:?}, returning C207!", client_key, e)).await;
                return create_special_command_response!(client_key, "C207"); // TODO >>> Improve the error handling of this error!
            },
        };
        if !(up_schedule.len() > 0) {
            return create_special_command_response!(client_key, "C207"); // If don't have any response to send send C207 that is a ping confirmation
        }

        let command_response = &up_schedule[0];
        let response_command = match Command::from_up_command(&command_response) {
            Ok(c) => c,
            Err(e) => {
                // TODO >>> Handle the invalid Commands cases
                logger.debug(format!("Command received during ping: {} is invalid, gives error: {:?}! Returning C207", command_response, e)).await;
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
pub async fn get_response(command: Command) -> Result<Option<Command>, BufferError> {
    let up_schedule: Vec<UpCommand> = enhanced_buffer::buffer_up_manager::buffer_up_get_scheduled_by_parity_id(&command.client_key, &command.parity_id).await?;

    if !(up_schedule.len() > 0) {
        return Ok(None);
    }

    let command_response = &up_schedule[0];
    let command_response_command = serde_json::from_str(command_response.command.as_str()).unwrap();
    let response_command = create_response_command!(command_response.client_key, command_response.parity_id, command_response.priority, command_response_command);
    enhanced_buffer::buffer_up_manager::buffer_up_remove_schedule_by_parity_id(&command.client_key, &response_command.parity_id);
    return Ok(Some(response_command));
}

const MAX_DATA_SIZE: usize = 10 * 1024 * 1024; // For example, 10 MB

#[derive(Debug)]
enum StreamError {
    WriteError(std::io::Error),
    WriteSizeError(std::io::Error),
    ConnectionClosed,
}

/// This function updates the node sync status attempt
/// > Important! - This function require globals:
/// - HOST_COMMAND_PATTERNS
/// - CLIENT_SYNC_CONTROLLER
/// Make sure to have them free before try to call this function to avoid blockages
async fn update_client_sync_attempt(client_key: &String, logger: &Logger) -> Result<(), ChangeStatusError> {
    let mut controller = CLIENTS_SYNC_CONTROLLER.lock().await;

    let client = controller.get_client(client_key).map_err(ChangeStatusError::from)?;

    {
        let mut actual_patterns = HOST_COMMAND_PATTERNS.lock().await;
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
            ClientStatusPoolError::MaxSyncAttemptsReached(s) => {
                handle_client_disconnect(&client_key); // Disconnect the client, what should trigger sync in all dependent ones
                return Err(ChangeStatusError::MaxSyncAttemptsReached(s));
            },
            ClientStatusPoolError::ClientAlreadySync(_) => match change_client_node_status_and_stream(client_key.clone(), NodeStatus::Online).await {
                Ok(_) => {},
                Err(e) => return Err(e),
            },
        }
    }

    return Ok(());
}

async fn get_response_or_error(command: Command) -> Command {
    let logger = acquire_logger!("Core");
    match get_response(command.clone()).await {
        Ok(r) => {
            if let Some(response) = r {
                if response.client_key == command.client_key {
                    return response;
                } else {
                    logger.info("Response is None!".to_string()).await;
                    create_special_command_confirmation!(command.client_key.clone(), command.parity_id.clone())
                }
            } else {
                logger.info("Response is None!".to_string()).await;
                create_special_command_confirmation!(command.client_key.clone(), command.parity_id.clone())
            }
        },
        Err(e) => {
            logger.exception(format!("Error trying to get the response, buffer error: {:?}", e)).await; // TODO >>> What to do here, logout client?
            create_special_command_confirmation!(command.client_key.clone(), command.parity_id.clone())
        },
    }
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
async fn handle_incoming(command: Command) -> std::io::Result<Option<Command>> {
    // Aquire logger to section Handle Conn
    let logger = acquire_logger!("Core");
    let mut client_key: String = "".to_string();

    // -> Before join in the loop, schedule a request of the client commands
    // let mut client: Option<Client> = None;

    // TODO >>> Remove the loop, make it reactive
    // TODO >>> Remove conver the strams senders into tx for the dispatcher thread

    loop {
        logger.debug(format!("Command received:\n{:?}\n", command)).await;

        let client_exists = match check_if_client_key_exists(command.client_key.clone()).await {
            Ok(ce) => ce,
            Err(e) => {
                let response: Command = create_error_command_response!(command.client_key, command.parity_id, format!("Error trying to check if the client key exists, the error was: {:?}", e.to_string()));
                logger.exception(format!("Error trying to check if the client key exists, the error was: {:?}", e.to_string())).await;
                return Ok(Some(response));
            },
        };

        // -> Drop clients that aren't in the white list:
        if !client_exists {
            // -> In case client isn't registered in the clients allowed
            let response: Command = create_error_command_response!(command.client_key, command.parity_id, "Your client isn't registered in the whitelist!");
            logger.exception(format!("WARNING: Client isn't registered, sending back: {:?}", response)).await;
            return Ok(Some(response));
        }

        client_key = command.client_key.clone();

        // -> ---------------------------------------------------------------------------------------------------------------------------------------------------
        // -> SYNC CONTROLLER:

        // -> GET CLIENT STATUS, SEE IF IT IS SYNC OR NOT
        let client_sync_status: Option<bool>;
        let client_last_sync: Option<DateTime<Utc>>;

        {
            let mut controller = CLIENTS_SYNC_CONTROLLER.lock().await;
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
            logger.debug(format!("Clients In Sync Controller: {:?}", controller)).await;
        }

        // -> Update Client Status
        update_last_contact(command.client_key.clone());

        // > Check if the max sync was reached
        // > if is first sync and yes, diconnect client
        // > if is not first sync and yes,change client status to not sync
        // > This should auto trigger sync to all clients that isn't sync in relation to the network map available for them

        // -> Refactored SYNC CONTROLLER:
        if let Some(sync) = client_sync_status {
            if !sync {
                logger.debug(format!("\nClient: {:?} isn't sync\n", &command.client_key)).await;

                let current_time = Utc::now();
                let should_attempt_sync = client_last_sync.map_or(true, |last_sync| current_time - last_sync > Duration::seconds(30));

                if should_attempt_sync {
                    logger.info(format!("Try to sync with: {}", command.client_key)).await;
                    send_network_available_commands(command.client_key.clone());

                    match update_client_sync_attempt(&command.client_key, &logger).await {
                        Ok(()) => {}, // Once sync, continue the default procedure loop!
                        Err(e) => {
                            logger.warn(format!("Trying to change the client status result in an error: {:?}", e));
                            // TODO >>> Enhance this error kind handling!
                        },
                    }

                    //> The new system only stream that the node connect here and is trying to sync so this new
                    //> node is with NotSyncYet status.
                    //> Then wen this node connects we change the status to Sync. If node isn't able to sync, we
                    //> change it to offline and disconnect it. Also another thing that we can do is impl a new Idle status that can be
                    //> represented as a pulsating orange color.
                } else if let Some(last_sync) = client_last_sync {
                    logger
                        .info(format!(
                            "WARNING: Client: {:?} not sync yet, trying again in: {:?} seconds!",
                            &command.client_key,
                            (Duration::seconds(30) - (current_time - last_sync)).num_seconds()
                        ))
                        .await;
                }
            } else {
                logger.debug(format!("\nClient: {:?} is sync!\n", &command.client_key)).await;
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
                command_patterns = HOST_COMMAND_PATTERNS.lock().await.clone();
            }

            // println!("[HOST][REGIRSTRED PATTERNS]:\n{:?}", command_patterns);

            logger.debug(format!("\nCommand.Command: {:?}", command.command)).await;
            logger.debug(format!("\nCommand.Command.function: {:?}", command.command.actf)).await;
            logger.debug(format!("Command function: {}", command.command.actf)).await;

            async fn special_fn_handling(command: &Command) -> Result<Command, String> {
                let logger = acquire_logger!("Core");
                let special_functions: Vec<String> = vec!["C202".to_string(), "C206".to_string()];

                if special_functions.contains(&command.command.actf) {
                    let response: Command = handle_special_functions(command.client_key.clone(), command.command.actf.clone()).await;
                    return Ok(response);
                }

                Err("Command Isn't A Special Function!".to_string())
            }

            /// Updates the tasks in the task table based in the
            /// incomming commands and the outcome tasks.
            async fn update_task_table(command: &Command, incoming: bool) {
                // Bypass (itisaspecialcase) that means bypass all internal tasks
                if command.parity_id == "itisaspecialcase" {
                    return;
                }

                // Bypass confirmation functions, now it only will delete tasks when the response itself is sended back to the receiver
                if command.command.actf == "C210" {
                    match command.command.mode {
                        CommandMode::Response => {
                            // let mut task = tasks_manager.get_node_task_by_id(&command.client_key, &command.parity_id.clone()).unwrap();
                            // task.received_conf(); // this store that the function was received by the target
                            return;
                        },
                        _ => {
                            // TODO >>> We can see about add the remove task here when the confirmation is confirmating the receive of the Response
                            return;
                        },
                    }
                }

                match command.command.mode {
                    CommandMode::Function => {
                        if incoming {
                            //-> Here the node key needs to always be the target since we are scheduling a task to the target not origin
                            if command.client_key != command.command.target.to_string() {
                                //> Case were we are receiving the command to redirect to a target
                                //> We pass the origin here just in case it isn't in the command instruction so then we can easily trace it
                                let node_task: NodeTask = NodeTask::new(command.client_key.clone(), command.parity_id.clone(), command.command.clone());
                                {
                                    let mut tasks_manager = TASKS_MANAGER.lock().await;
                                    tasks_manager.add_task_to_node(&command.command.target.as_pure_string(), node_task).unwrap();
                                }
                            }
                            //-> Here the node key needs to always be the target since we are scheduling a task to the target not origin
                            if command.client_key == command.command.target.to_string() {
                                //> Handle the cases were we are sending some comand to a target
                                {
                                    let mut tasks_manager = TASKS_MANAGER.lock().await;
                                    let mut task = tasks_manager.get_node_task_by_id(&command.command.target.as_pure_string(), &command.parity_id.clone()).unwrap();
                                    task.sended();
                                }
                            }
                        }
                    },
                    CommandMode::Response => {
                        if !incoming {
                            // -> Here theoretically the client_key of the command is the target of the command that cause this response
                            if command.client_key == command.command.target.to_string() {
                                {
                                    let mut tasks_manager = TASKS_MANAGER.lock().await;
                                    println!("Attempt to remove task: {} from node: {}", &command.parity_id, &command.client_key);
                                    tasks_manager.remove_task_from_node(&command.client_key, &command.parity_id.clone());
                                    println!("Task: {} removed", &command.parity_id);
                                }
                            }

                            // < C210 commands are always with (itisaspecialcase) parity_id by filtering the special case we can filter them
                            // TODO >>> Verify isn't just a confirmation
                            // TODO >>> Verify if the response matches some command
                            // TODO >>> Remove the command of the taskss
                        }
                    },
                }
            }

            //> Early return for command type:
            update_task_table(&command, true).await; // This is important to be here to handle the cases were we need to verify the confirmation received
            match &command.command_type() {
                CommandType::SpecialFunction => {
                    // -> HANDLE SPECIAL FUNCTION CASES:
                    match special_fn_handling(&command).await {
                        Ok(c) => {
                            return Ok(Some(c));
                        },
                        Err(e) => {
                            logger.exception(e).await; // TODO >>> Maybe implement something here to prevent spamming the buffer with warns if the methods aren't allowed
                        },
                    };
                    continue;
                },
                _ => {},
            }

            // -> HANDLE HOST FUNCTIONS - DIRECT AND EXTERNAL FUNCTION:
            match &command.command.target {
                //->  Redirect cases:
                CommandTarget::ClientKey(target) => {
                    // < WARNING: This locks command_patterns!

                    // > EARLY REMOVE FROM DOWN BUFFER TO AVOID REPETITION ERRORS SINCE THE COMMAND IS ALREADY BEING PROCESSED
                    enhanced_buffer::buffer_down_manager::buffer_down_remove_schedule_by_parity_id(command.client_key.clone(), command.parity_id.clone());

                    let command_is_not_registry: bool = match enhanced_buffer::buffer_up_manager::check_if_parity_id_is_registered(command.parity_id.clone(), target.clone()).await {
                        Ok(b) => b,
                        Err(e) => {
                            logger.exception(format!("Error trying to check if the parity id is registered, command: {:?}, Error: {:?}", command, e)).await;
                            let res: Command = create_error_command_response!(command.client_key.clone(), command.parity_id, format!("Error trying to check if the parity id is registered for the command: {:?}", command));
                            return Ok(Some(res));
                        },
                    };

                    //> HANDLE COMMANDS WITH RESPONSE:
                    if !command_is_not_registry {
                        logger.warn(format!("Command {}, already have a response!", command.parity_id.clone())).await;
                        return Ok(Some(get_response_or_error(command.clone()).await));
                    // > HANDLE COMMANDS WITHOUT RESPONSE:
                    } else {
                        let mut command_patterns: NetworkMap;
                        {
                            command_patterns = HOST_COMMAND_PATTERNS.lock().await.clone();
                        }
                        let commands: Vec<CommandVariant> = redirect_commands_processing(&command, target, &mut command_patterns).await;
                        for command in commands {
                            match command {
                                CommandVariant::Command(res) => {
                                    update_task_table(&res, false).await;
                                    return Ok(Some(res));
                                },
                                CommandVariant::UpCommand(up) => {
                                    enhanced_buffer::buffer_up_manager::buffer_up_schedule(up);
                                },
                                CommandVariant::DownCommand(_) => {
                                    panic!("Doesn't is expected to receive DownCommand here, smething is wrong!")
                                },
                            }
                        }
                    }
                },
                CommandTarget::Host => {
                    //> SEND RESPONSE BACK - HERE IT CAN BE COMMAND RESPONSES OR CONFIRMATIONS
                    // < WARNING: This locks command_patterns!
                    let res: Command = host_commands_processing(&command).await;
                    update_task_table(&res, false).await; // update to the tasks is done here in case res is a response to something pendent to this client
                    logger.debug(format!("Sending back: {:?}", res)).await;
                    return Ok(Some(res));
                },
                CommandTarget::Origin => {
                    let mut command: Command = command.clone();
                    let real_origin: String;

                    // < WARNING: This locks command_patterns!
                    // < WANING: This locks tasks_manager!

                    {
                        let mut tasks_manager = TASKS_MANAGER.lock().await;
                        println!("Current parity id to match to a task: {}", &command.parity_id);
                        // tasks_manager.show_node_tasks(&command.client_key);
                        real_origin = match tasks_manager.get_node_task_origin(&command.client_key, &command.parity_id) {
                            Ok(ro) => ro,
                            Err(e) => {
                                logger.exception(format!("Error trying to get the node task origin for command: {:?}, Error: {:?}", command, e)).await;
                                let res: Command = create_error_command_response!(command.client_key.clone(), command.parity_id, format!("Error trying to check if the parity id is registered for the command: {:?}", command));
                                return Ok(Some(res));
                            },
                        };
                    }

                    // > EARLY REMOVE FROM DOWN BUFFER TO AVOID REPETITION ERRORS SINCE THE COMMAND IS ALREADY BEING PROCESSED
                    enhanced_buffer::buffer_down_manager::buffer_down_remove_schedule_by_parity_id(command.client_key.clone(), command.parity_id.clone());

                    let command_is_not_registry: bool = match enhanced_buffer::buffer_up_manager::check_if_parity_id_is_registered(command.parity_id.clone(), real_origin.clone()).await {
                        Ok(cinr) => cinr,
                        Err(e) => {
                            logger.exception(format!("Error trying to check if the parity id is registered for the command: {:?}", command)).await;
                            let res: Command = create_error_command_response!(command.client_key.clone(), command.parity_id, format!("Error trying to check if the parity id is registered for the command: {:?}", command));
                            return Ok(Some(res));
                        },
                    };

                    //> HANDLE COMMANDS WITH RESPONSE:
                    if !command_is_not_registry {
                        logger.warn(format!("Command {}, already have a response!", command.parity_id.clone())).await;
                        return Ok(Some(get_response_or_error(command.clone()).await));
                    // > HANDLE COMMANDS WITHOUT RESPONSE:
                    } else {
                        println!("Find real origin to: {} that is: {}", &command.parity_id, real_origin);
                        command.command.target = CommandTarget::ClientKey(real_origin.to_string().clone());

                        // Get the command patterns map index with all network map variants:
                        let mut command_patterns: NetworkMap;
                        {
                            command_patterns = HOST_COMMAND_PATTERNS.lock().await.clone();
                        }

                        let commands: Vec<CommandVariant> = redirect_commands_processing(&command, &real_origin.to_string(), &mut command_patterns).await;

                        for com in commands {
                            match com {
                                CommandVariant::Command(res) => {
                                    println!("Command: {} swaped to origin: {}", &command.parity_id, real_origin);
                                    println!("New command casted: \n{:#?}\n", &res);

                                    if res.command.status == "Failure" {
                                        {
                                            let mut tasks_manager = TASKS_MANAGER.lock().await;
                                            println!("Attempt to remove task: {} from node: {} cause it cause failure", &command.parity_id, &command.client_key);
                                            tasks_manager.remove_task_from_node(&command.client_key, &command.parity_id.clone());
                                            println!("Task: {} removed", &command.parity_id);
                                        }
                                    } else {
                                        update_task_table(&res, false).await; // update to the tasks is done here in case res is a response to something pendent to this client
                                    }

                                    println!("Tasks updated");

                                    {
                                        let mut tasks_manager = TASKS_MANAGER.lock().await;
                                        tasks_manager.show_node_tasks(&command.client_key);
                                    }

                                    return Ok(Some(res));
                                },
                                CommandVariant::UpCommand(up) => {
                                    enhanced_buffer::buffer_up_manager::buffer_up_schedule(up);
                                },
                                CommandVariant::DownCommand(_) => {
                                    panic!("Doesn't is expected to receive DownCommand here, smething is wrong!")
                                },
                            };
                        }
                    }
                },
                _ => {
                    // -> HANDLE THE CASE WERE A COMMAND DOES EXISTS HERE IN HOST NOR IN ANY NODE THAT CLIENT HAS PERMISSION
                    let res: Command = create_error_command_response!(
                        command.client_key.clone(),
                        command.parity_id,
                        format!("Command: {:?}, isn't valid, you cant send a command to host with a target origin, this isn't allowed!", command.command)
                    );
                    update_task_table(&res, false).await; // update to the tasks is done here in case res is a response to something pendent to this client
                    logger.debug(format!("Sending back: {:?}", &res)).await;
                    let client_key = res.client_key.clone();
                    return Ok(Some(res));
                    handle_client_disconnect(&client_key);
                    break;
                },
            }
        }
    }

    handle_client_disconnect(&client_key);

    return Ok(None);
}

async fn handle_connection(mut stream: TcpStream, unit_senders: UnitSenders, client_txs: ClientMap) -> std::io::Result<()> {
    // Aquire logger to section Handle Conn
    let logger = acquire_logger!("Core");
    let mut client_key: String = "".to_string();

    // Create the channel for sending data back to this client from the transposer.
    let (tx_to_client, mut rx_from_transposer) = mpsc::channel::<String>(32);
    let client_id = uuid::Uuid::new_v4().to_string();

    // TODO >>> Late initialize the txs with the client id received from the client, but do all verifications first

    // Insert into the global client map
    {
        let mut guard = client_txs.lock().await;
        guard.insert(client_id.clone(), tx_to_client);
    }

    let mut cloned_ref_client_txs: ClientMap = Arc::clone(&client_txs);

    // Split the stream into reading and writing parts
    let (mut reader, mut writer) = stream.into_split();

    let client_id_clone = client_id.clone();

    let read_task = tokio::spawn(async move {
        loop {
            let mut size_buffer = [0u8; 4];

            // Read exactly 4 bytes to get the size
            if let Err(e) = reader.read_exact(&mut size_buffer).await {
                match e.kind() {
                    ErrorKind::UnexpectedEof => {
                        println!("Client {} disconnected", client_id_clone);
                    },
                    _ => {
                        logger.exception(format!("Failed to read size from client {}: {:?}", client_id_clone, e)).await;
                    },
                }
                handle_client_disconnect(&client_key);
                break;
            }

            let data_size = u32::from_be_bytes(size_buffer) as usize;
            if data_size > MAX_DATA_SIZE {
                logger.exception(format!("Client {} sent data too large: {}", client_id_clone, data_size)).await;
                handle_client_disconnect(&client_key);
                break;
            }

            let mut data_buffer = vec![0u8; data_size];
            if let Err(e) = reader.read_exact(&mut data_buffer).await {
                logger.exception(format!("Failed to read payload from client {}: {}", client_id_clone, e)).await;
                handle_client_disconnect(&client_key);
                break;
            }

            let buffer_string = String::from_utf8_lossy(&data_buffer).trim_end_matches(|c| c == '\n' || c == '\r' || c == '\0').to_string();

            match serde_json::from_str::<Command>(&buffer_string) {
                Ok(command) => {
                    // 🔁 Await the command handler

                    // println!("Entering in handle incoming command: {:?}", command);

                    match handle_incoming(command).await {
                        Ok(response) => {
                            if let Some(res) = response {
                                let command_response_json: String = json!(res).to_string();
                                let guard = cloned_ref_client_txs.lock().await;
                                if let Some(tx) = guard.get(&client_id_clone) {
                                    // TODO >>> Verify if the tx will correctly send the response for the writer.
                                    // > This is done this way in order to keep the writer centralized and allow it to receive writing tasks
                                    // > from multiple sources without cause some kind of racing condition between the senders, this way
                                    // > We make the structure simpler and more event driven, more reactive and simpler than having to have multiple layers of nested senders all over the place.
                                    if let Err(e) = tx.send(command_response_json).await {
                                        logger.exception(format!("Error sending response to client {}: {}", client_id_clone, e)).await;
                                    }
                                } else {
                                    logger.exception(format!("No client sender found for {}", client_id_clone)).await;
                                }
                            } else {
                                // TODO >>> Handle the cases were the some is none!
                                panic!("Handle incomming response should not be None something is wrong!")
                            }
                        },
                        Err(e) => {
                            // TODO >>> Send an copy of the tx that connects to the client socket reactive task rx
                            logger.warn(format!("Error handling command for client {}: {}", client_id_clone, e)).await;
                        },
                    }
                },
                Err(e) => {
                    logger.warn(format!("Failed to deserialize command from client {}: {}", client_id_clone, e)).await;
                },
            }
        }
    });

    // >---------------------------------------------------------------------------------------------------------
    let mut client_key: String = "".to_string();
    let logger = acquire_logger!("Core");

    // let command_response_json: String = json!(data).to_string();

    let client_id_clone = client_id.clone();

    // A task for writing responses from the transposer back to the client
    let write_task = tokio::spawn(async move {
        while let Some(command_response_json) = rx_from_transposer.recv().await {
            if command_response_json.trim().is_empty() || command_response_json.trim() == "null" {
                continue;
            }

            logger.debug(format!("📨 Sending to client {}: {}", client_id_clone, command_response_json)).await;

            // Check if the connection was closed
            if writer.peer_addr().is_err() {
                return Err(StreamError::ConnectionClosed);
            }

            let data_size: u32 = command_response_json.len() as u32;
            let size_buffer: [u8; 4] = data_size.to_be_bytes();

            // Send the size of the data
            if let Err(e) = writer.write_all(&size_buffer).await {
                logger.exception(format!("Error writing size to client {}: {}", client_id_clone, e)).await;
                handle_client_disconnect(&client_key);
                break;
            }

            // Send the actual data
            if let Err(e) = writer.write_all(command_response_json.as_bytes()).await {
                logger.exception(format!("Error writing to client {}: {}", client_id_clone, e)).await;
                handle_client_disconnect(&client_key);
                break;
            }
        }

        Ok::<_, StreamError>(()) // <- Fix: ensure this async block returns a Result
    });

    // -> Wait for either the reading or writing side to finish
    tokio::select! {
        _ = read_task => {},
        _ = write_task => {},
    }

    // -> Once either side is done, remove the client from the map
    {
        let mut guard = client_txs.lock().await;
        guard.remove(&client_id);
    }

    let logger = acquire_logger!("Core");
    logger.info(format!("🔌 Removed client {} from map.", client_id)).await;

    Ok(())
}
