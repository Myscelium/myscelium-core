use crate::common::enhanced_buffer;
use crate::common::enhanced_buffer::buffer_down_manager::DownCommand;
use crate::common::enhanced_buffer::utilities::{Command, CommandError, CommandInstructions, CommandMode, CommandOrigin, CommandStatus, CommandTarget, CommandType};

use super::client_logger::log_handler::Logger;

use crate::{CLIENT_BUFFER_ACTIVATION_CONTROLLER, CLIENT_IS_SYNC, CLIENT_NODE_CONFIGS, MEDIAN_CON_RESP_TIME};

use dashmap::DashMap;
use indexmap::IndexMap;
use serde_json::json;
use serde_json::{from_str, Value};
use std::collections::HashMap;
use std::io::{self, Write};
use std::io::{ErrorKind, Read};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::{cmp, thread};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{
    tcp::{OwnedReadHalf, OwnedWriteHalf},
    TcpStream,
};
use tokio::select;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::Notify;
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{sleep, Instant};

use crate::CLIENT_IS_CONNECTED;
use crate::CLIENT_IS_RUNNING;
use crate::CLIENT_LOG_LEVEL;
use crate::CLIENT_NODE_KEY;

use lazy_static::lazy_static;
use parking_lot::Mutex;
// use std::sync::Mutex;

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

use crate::common::structs::available_commands::{CommandPatterns, Node};
// use crate::CLIENT_ID;

use dashmap::DashSet; // 2. lock-free, sharded set
use once_cell::sync::Lazy; // 1. one-time init

lazy_static! {
    static ref HOST_ALLOWED_COMMANDS: Arc<Mutex<HashMap<String, Value>>> = {
        let json_str = r#"{
            "get_symbols_data": {
                "symbols_data": {
                    "data-type": "str",
                    "symbols": "str",
                    "start-ts": "float",W
                    "end-ts": "float"
                }
            },
            "get_other_symbols_data": {
                "symbols_data": {
                    "data-type": "str",
                    "symbols": "str",
                    "start-ts": "float",
                    "end-ts": "float"
                }
            }
        }"#;

        let command_patterns: HashMap<String, Value> = from_str(json_str).unwrap();
        Arc::new(Mutex::new(command_patterns))
    };
    static ref CLIENT_ID: Arc<Mutex<String>> = Arc::new(Mutex::new(' '.to_string()));
    static ref COMMANDS_SENT_WAITING_RESPONSE: Lazy<DashMap<String, Instant>> = Lazy::new(DashMap::new);
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct WaitingResponse {
    parity_id: String,
    timestamp: Instant,
}

async fn contains(id: &str) -> bool {
    COMMANDS_SENT_WAITING_RESPONSE.contains_key(id)
}

async fn latency(id: &str) -> Option<std::time::Duration> {
    COMMANDS_SENT_WAITING_RESPONSE.get(id).map(|ts| ts.elapsed())
}

// >-------------------------------------------------------------------------------------------------------------------------------------------

// -> Socket Interactive Functions:

use crate::common::enhanced_buffer::history::register::register::initialize_buffer_history;

/// Initializes the client buffer by setting up the necessary tables.
///
/// This function is responsible for initializing the buffer tables for both
/// up and down commands. If the tables aren't already initialized, they will be
/// created at the specified `buffer_location`.
///
/// # Arguments
/// - `buffer_location`: The location where the buffer database will be initialized.
///
/// # Side Effects
/// - If not already initialized, the function will create and initialize the buffer database
///   at the specified location.
pub fn initialize_client_buffer(buffer_location: String) {
    println!("initializing the buffer database into: {}buffer.db, if not initialized!", buffer_location);

    initialize_buffer_history(&buffer_location);

    // -> INITIALIZE TABLES
    enhanced_buffer::buffer_down_manager::buffer_down_initialize_table(buffer_location.clone());
    enhanced_buffer::buffer_up_manager::buffer_up_initialize_table(buffer_location.clone());

    println!("All buffer initialized successfully!");

    return;
}

// Keep the thread alive until HOST_IS_RUNNING is set to false
// if !CLIENT_IS_RUNNING.load(Ordering::SeqCst) {
//     print!("running is set to false, skipping");
//     break;
// }

// The incoming method is called on the listener, which returns an iterator that gives us a sequence of
// TCP streams (representing a series of connections). The server will then handle each connection in a loop.

// handle_connection is a function that handles each TCP stream. It reads from the stream into a buffer,
// then writes the contents of the buffer back to the stream.

/// Retrieves the available command patterns registered for the socket client.
///
/// This function provides access to the current command patterns registered in the socket client.
/// These patterns dictate how commands are recognized and processed.
///
/// # Returns
/// - A `HashMap` containing the available command patterns.
pub fn get_available_handlers_registered() -> HashMap<String, IndexMap<std::string::String, std::string::String>> {
    let global_command_patterns: HashMap<String, IndexMap<std::string::String, std::string::String>>;

    {
        println!("[CLIENT][GLOBAL][Try Lock] - CLIENT_NODE_CONFIGS");
        let command_patterns = CLIENT_NODE_CONFIGS.lock();
        println!("[CLIENT][GLOBAL][Lock] - CLIENT_NODE_CONFIGS");
        global_command_patterns = match command_patterns.get_node_handlers() {
            Ok(h) => h,
            Err(e) => {
                println!("Get a error while trying to get the client node handlers!");
                panic!("Get a error while trying to get the client node handlers!");
            },
        };
    }
    println!("[CLIENT][GLOBAL][Release] - CLIENT_NODE_CONFIGS");

    return global_command_patterns;
}

// > --------------------------------------------------------------------------------------------------------------------------------------

// -> Socket client functionality structures:

// #[derive(Serialize, Deserializer, Debug)] is an attribute that automatically
// derives the Serialize and Deserialize traits from the serde crate, witch allow
// the struct to be converted to and from JSON.

// The Debug Trait, is also derived, which allows the structure to be printed fro debugging purposes

/// Represents possible responses from the server.
///
/// This enum is used to capture the different types of responses that the server can send.
///
/// Variants:
/// - `Command`: Represents a valid command response from the server.
/// - `None`: Represents an absence of response or an invalid response.

macro_rules! create_special_command {
    ($client_key:expr, $command_mode:expr, $special_command:expr) => {{
        let command_instructions = CommandInstructions::new(
            $command_mode,
            CommandType::SpecialFunction,
            CommandTarget::Host,
            CommandStatus::Success,
            CommandOrigin::ClientKey($client_key.clone()),
            $special_command.to_string(),
            HashMap::new(),
            "".to_string(),
            None, // Not required here
            None, // Not required here
            None, // Not required here
            true,
        );

        let command = Command {
            client_key: $client_key.clone().to_string(),
            parity_id: "itisaspecialcase".to_string(),
            priority: 11,
            command: command_instructions,
        };
        command
    }};
}

/// Verifies the connection to the server by sending a special command and checking the response.
///
/// This function sends a special command `"C202"` to the server and expects a response with the function `"C200"`.
/// If the response matches the expectation, it means the connection is verified.
///
/// # Arguments
/// - `stream`: A mutable reference to the active TcpStream.
///
/// # Returns
/// - `true` if the connection is verified successfully.
/// - `false` otherwise.
///
/// # Behavior
/// - The function logs any unexpected responses or errors.
async fn verify_connection(stream: &mut TcpStream, client_key: &String) -> bool {
    let logger = acquire_logger!("Core");

    let command = create_special_command!(client_key.clone(), CommandMode::Function, "C202");

    println!("\nTry to verify connection!");

    {
        let command_response_json = json!(command).to_string();
        let data_size = command_response_json.len() as u32;
        let size_buffer = data_size.to_be_bytes();

        // Send the size of the data
        match stream.write(&size_buffer).await {
            Ok(_) => {},
            Err(e) => {
                println!("Error sending data lenght to client: {:?}, the error was:  {:?}", command.client_key, e);
            },
        };

        // println!("Data lenght: {:?}", data_size);

        // Send the actual data
        match stream.write(command_response_json.as_bytes()).await {
            Ok(_) => {},
            Err(e) => {
                println!("Error sending data to host: {:?} the error was: {:?}", command.client_key, e);
            },
        };

        println!("Connection verification sended!");
    }

    let mut size_buffer = [0; 4];

    // Read the size of the incoming data
    let data_size = match stream.read_exact(&mut size_buffer).await {
        Ok(_) => u32::from_be_bytes(size_buffer) as usize,
        Err(e) => {
            eprintln!("Failed to read from the stream: {:?}", e);
            // Handle the error, e.g., by returning from the function or taking corrective action
            return false; // or handle differently
        },
    };

    println!("Confirmation data received!");

    if data_size > MAX_DATA_SIZE {
        logger.exception(format!("Data size too large: {}", data_size));
        return false; // TODO >>> Close connection or handle appropriately
    }

    // Allocate a buffer of the appropriate size
    let mut data_buffer = vec![0; data_size];

    let buffer_string = match stream.read_exact(&mut data_buffer).await {
        Ok(_) => String::from_utf8_lossy(&data_buffer).trim_end_matches(|c| c == '\n' || c == '\r' || c == '\0').to_string(),
        Err(e) => {
            eprintln!("Failed to read from the stream: {:?}", e);
            // Handle the error, e.g., by returning from the function or taking corrective action
            return false; // or handle differently
        },
    };

    println!("Confirmation data decoded!");
    println!("String: {:?}", &buffer_string);

    let command: Command = match serde_json::from_str(&buffer_string) {
        Ok(c) => c,
        Err(e) => {
            // TODO >>> Create a form to do the error handling of this!!! # Very important!
            logger.exception(format!("Error trying to downcast the command from str: {:?}", e));
            panic!("Error trying to downcast the command from str: {:?}", e)
        },
    };

    logger.debug(format!("Received: {:?}", command));

    if command.command.actf == "C200" {
        println!("Connected!\n");
        return true;
    } else {
        println!("Error in connection verification, not connected!\n");
        return false;
    }

    // logger.warn(format!("The function name is not found or not a string."));
}

const MAX_DATA_SIZE: usize = 10 * 1024 * 1024; // For example, 10 MB

// Define a custom error type for stream-related errors
#[derive(Debug)]
pub enum StreamError {
    WriteError(std::io::Error),
    WriteSizeError(std::io::Error),
    ConnectionClosed,
    ReadSizeError(std::io::Error),
    ReadDataError(std::io::Error),
    SendError(mpsc::error::SendError<String>),
    FrameTooLarge(usize),
    InvalidUtf8(std::string::FromUtf8Error),
}

pub fn set_client_uid(client_key: String) {
    let logger = acquire_logger!("Core");
    {
        logger.debug(format!("[CLIENT][GLOBAL][Try Lock] - CLIENT_ID"));
        let mut c_uid = CLIENT_ID.lock();
        logger.debug(format!("[CLIENT][GLOBAL][Lock] - CLIENT_ID"));
        *c_uid = client_key
    }
    logger.debug(format!("[CLIENT][GLOBAL][Release] - CLIENT_ID"));
}

/// Sends a command to the server and waits for a response.
///
/// Before sending the command, the function verifies the connection using the `verify_connection` function.
/// If the connection is not verified, the function returns `Response::None`.
///
/// # Arguments
/// - `stream`: A mutable reference to the active TcpStream.
/// - `command`: The `Command` object to be sent to the server.
///
/// # Returns
/// - A `Response` variant containing the server's response.
///
/// # Behavior
/// - If the connection is not verified, the function logs the event and returns `Response::None`.
pub async fn sender(mut writer: OwnedWriteHalf, mut rx: Receiver<String>) -> Result<(), StreamError> {
    while let Some(command_response_json) = rx.recv().await {
        // let command_response_json = json!(command).to_string();
        let data_size = command_response_json.len() as u32;
        let size_buffer = data_size.to_be_bytes();

        // Verify if stil connected
        if writer.peer_addr().is_err() {
            return Err(StreamError::ConnectionClosed);
        }

        // Send the size of the data
        writer.write_all(&size_buffer).await.map_err(StreamError::WriteSizeError)?;

        // Send the actual data
        writer.write_all(command_response_json.as_bytes()).await.map_err(StreamError::WriteError)?;
    }
    Ok(())
}

async fn receiver(mut reader: OwnedReadHalf, mut tx: Sender<String>) -> Result<(), StreamError> {
    let logger = acquire_logger!("Receiver");
    loop {
        // Read length prefix:
        let size = match reader.read_u32().await {
            Ok(n) => n as usize,
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => {
                logger.info("Peer closed connection".to_string());
                break;
            },
            Err(e) => return Err(StreamError::ReadSizeError(e)),
        };

        // Check bounds using your existing error:
        if size == 0 {
            continue;
        }
        if size > MAX_DATA_SIZE {
            logger.exception(format!("Frame too large (max = {}, got = {})", MAX_DATA_SIZE, size));
            return Err(StreamError::ConnectionClosed);
        }

        // Read payload:
        let mut buf = vec![0u8; size];
        reader.read_exact(&mut buf).await.map_err(StreamError::ReadDataError)?;

        // Convert & send:
        let msg = String::from_utf8_lossy(&buf).trim_end_matches(|c| c == '\n' || c == '\r' || c == '\0').to_string(); // TODO >>> Maybe switch from lossy conversion to strict conversion

        // tx.send(msg).await.map_err(|_| StreamError::ConnectionClosed)?;

        if msg.len() == 0 {
            continue;
        }

        let response: Command = serde_json::from_str(&msg).unwrap();

        // -> Match command status:
        match response.command.status {
            CommandStatus::Failure => {
                logger.exception(format!("\nAn error occurred in host, the error was: {}\n", response.command.message));
                enhanced_buffer::buffer_up_manager::buffer_up_remove_schedule_by_parity_id(&response.client_key, &response.parity_id);
                CLIENT_IS_RUNNING.store(false, Ordering::SeqCst);
                CLIENT_IS_CONNECTED.store(false, Ordering::SeqCst);
                CLIENT_IS_SYNC.store(false, Ordering::SeqCst);
                break; // TODO >>> ? Do a proper error handling of this scenario and try to recover, or atleast indentify the severity of the failure
            },
            CommandStatus::Success => {},
        }

        // -> Dispatch based on command type:
        match response.command_type() {
            CommandType::ExternalFunction => {
                let down_command = DownCommand::from_command(response);
                logger.debug(format!("[Socket Client] - Receives Data.. : {:?}", down_command));
                enhanced_buffer::buffer_down_manager::buffer_down_schedule(&down_command);
                {
                    let react_actv = CLIENT_BUFFER_ACTIVATION_CONTROLLER.lock();
                    react_actv.start();
                }
                continue;
            },
            CommandType::DirectFunction => {
                // TODO >>> Need to actualize this to the new patter like Response handler to redirect works as intended!
                // > Also we can use a similar system to sync multiple hosts
                logger.info(format!("[Socket Client] - Received a direct function!:\n {:?}", response.command.actf));
                let down_command = DownCommand::from_command(response);
                enhanced_buffer::buffer_down_manager::buffer_down_schedule(&down_command);
                {
                    let react_actv = CLIENT_BUFFER_ACTIVATION_CONTROLLER.lock();
                    react_actv.start();
                }
                continue;
            },
            CommandType::SpecialFunction => {
                if response.parity_id != "itisaspecialcase" {
                    if response.command.actf == "C210".to_string() {
                        logger.info(format!("Received Confirmation! Removing command {} of client: {} from buffer up", response.parity_id, response.client_key));
                        COMMANDS_SENT_WAITING_RESPONSE.remove(&response.parity_id);
                        enhanced_buffer::buffer_up_manager::buffer_up_remove_schedule_by_parity_id(&response.client_key, &response.parity_id);
                        continue;
                    }
                }

                if response.command.actf == "C207".to_string() {
                    logger.debug(format!("Receive ping confirmation"));
                    continue;
                }

                logger.debug(format!("Received a unknow special function"));
                break; // TODO >>> maybe switch to panic to prevent hide an error.
            },
        }

        logger.debug(format!("Invalid command!"));
        break;
    }

    Ok(())
}

/// Sends a ping request to the server.
///
/// This function sends a special command `"C206"` to ping the server. It utilizes the `send` function
/// to send the ping request and then processes the received response using the `handle_response` function.
///
/// # Arguments
/// - `stream`: A mutable reference to the active TcpStream.
///
/// # Returns
/// - An `Option<DownCommand>` containing the processed command if there's any, or `None` otherwise.
///
/// # Behavior
/// - If the `CLIENT_IS_RUNNING` global flag is set to false, the function will immediately return `None`.
pub async fn heartbeat(mut sender_tx: Sender<String>, client_key: &String) -> Result<(), tokio::sync::mpsc::error::SendError<std::string::String>> {
    println!("Trying to acquire logger in heartbeat!");
    let logger = acquire_logger!("[CLIENT][SOCKET][HEARTBEAT]");
    loop {
        let command_to_request: Command = create_special_command!(client_key, CommandMode::Function, "C206");
        let command_string: String = serde_json::to_string(&command_to_request).unwrap();

        logger.debug("Sending ping C206".to_string());

        // Send command and measure time
        tokio::time::sleep(Duration::from_millis(120)).await;
        sender_tx.send(command_string).await?;

        // TODO >>> Save the time that the command were send, and then, in a global, save the time when the command receive confirmation have arrived.
        // let start = Instant::now();
        // let duration = start.elapsed();
    }
    Ok(())
}

async fn handle_stream_error(e: StreamError, client_key: &String) {
    let logger = acquire_logger!("Core");

    logger.exception(format!("A exception occurred! Error: {:?}", e));
    CLIENT_IS_CONNECTED.store(false, Ordering::SeqCst);
    CLIENT_IS_SYNC.store(false, Ordering::SeqCst);
    match e {
        StreamError::ConnectionClosed => {
            logger.exception(format!("[CLIENT][SOCKET][CLOSE CONNECTION] - {}", &client_key));
        },
        StreamError::WriteError(e) => {
            logger.exception(format!("[CLIENT][SOCKET][WRITE ERROR] - {:?}", e));
            logger.exception(format!("[CLIENT][SOCKET][CLOSE CONNECTION] - {}", &client_key));
            match e.kind() {
                io::ErrorKind::ConnectionReset => {
                    // TODO >>> Properly Handle this error
                },
                _ => {
                    CLIENT_IS_RUNNING.store(false, Ordering::SeqCst);
                    println!("Other error occurred")
                },
            }
        },
        StreamError::WriteSizeError(e) => {
            logger.exception(format!("[CLIENT][SOCKET][WRITE SIZE ERROR] - {:?}", e));
            logger.exception(format!("[CLIENT][SOCKET][CLOSE CONNECTION] - {}", &client_key));
            match e.kind() {
                io::ErrorKind::ConnectionReset => {
                    // TODO >>> Properly Handle this error
                },
                _ => {
                    CLIENT_IS_RUNNING.store(false, Ordering::SeqCst);
                    println!("Other error occurred")
                },
            }
        },
        StreamError::ReadSizeError(e) => {
            logger.exception(format!("[CLIENT][SOCKET][READ SIZE ERROR] - {:?}", e));
            logger.exception(format!("[CLIENT][SOCKET][CLOSE CONNECTION] - {}", &client_key));
            match e.kind() {
                io::ErrorKind::ConnectionReset => {
                    // TODO >>> Properly Handle this error
                },
                _ => {
                    CLIENT_IS_RUNNING.store(false, Ordering::SeqCst);
                    println!("Other error occurred")
                },
            }
        },
        StreamError::ReadDataError(e) => {
            logger.exception(format!("[CLIENT][SOCKET][READ DATA ERROR] - {:?}", e));
            logger.exception(format!("[CLIENT][SOCKET][CLOSE CONNECTION] - {}", &client_key));
            match e.kind() {
                io::ErrorKind::ConnectionReset => {
                    // TODO >>> Properly Handle this error
                },
                _ => {
                    CLIENT_IS_RUNNING.store(false, Ordering::SeqCst);
                    println!("Other error occurred")
                },
            }
        },
        _ => {},
    };
}

// Change your signature to async and return the two halves
async fn connect(address: String) -> Option<TcpStream> {
    let logger = acquire_logger!("Core");

    // Connect asynchronously
    let stream = match TcpStream::connect(address.clone()).await {
        Ok(s) => s,
        Err(e) => match e.kind() {
            ErrorKind::ConnectionRefused => {
                logger.debug(format!("Can't connect to host!"));
                return None;
            },
            _ => {
                logger.debug(format!("Unhandled error: {}", e));
                panic!("Unhandled error: {}", e)
            },
        },
    };

    Some(stream)
}

async fn first_connection(address: String, client_key: &String) -> TcpStream {
    let mut stream: TcpStream;

    let mut backoff = Duration::from_secs(1);
    const MAX_BACKOFF: Duration = Duration::from_secs(30);
    let mut last_attempt_time: Instant = Instant::now() - backoff;

    loop {
        let now = Instant::now();
        if now.duration_since(last_attempt_time) >= backoff {
            // try to connect again
            if let Some(st) = connect(address.clone()).await {
                stream = st;
                if verify_connection(&mut stream, &client_key).await {
                    CLIENT_IS_CONNECTED.store(true, Ordering::SeqCst);
                    break;
                }
            }

            // Update last attempt time
            last_attempt_time = now
        } else {
            let dif: Duration = backoff - (now - last_attempt_time);
            println!("Trying to connect again in: {:?} secs", dif.as_secs());
        }
        sleep(backoff).await;
        backoff = cmp::min(backoff * 2, MAX_BACKOFF);
    }

    return stream;
}

async fn reconnect(address: String, client_key: &String) -> TcpStream {
    let mut stream: TcpStream;
    let delay: u64 = 30u64;
    let mut last_attempt_time: Instant = Instant::now() - Duration::from_secs(delay);

    loop {
        if !CLIENT_IS_CONNECTED.load(Ordering::SeqCst) {
            let now = Instant::now();
            if now.duration_since(last_attempt_time) >= Duration::from_secs(delay) {
                // try to connect again
                if let Some(st) = connect(address.clone()).await {
                    stream = st;
                    if verify_connection(&mut stream, &client_key).await {
                        CLIENT_IS_CONNECTED.store(true, Ordering::SeqCst);
                        continue;
                    }
                }
                // Update last attempt time
                last_attempt_time = now
            } else {
                let dif: Duration = Duration::from_secs(delay) - (now - last_attempt_time);
                println!("Trying to connect again in: {} secs", dif.as_secs());
            }
            tokio::time::sleep(Duration::from_secs(1u64));
            continue;
        }
    }

    return stream;
}

/// Initializes the client and sets up communication with the specified server address.
///
/// This function connects to the provided server address, and then periodically checks
/// for scheduled commands and sends them to the server. The function also spawns a
/// background thread to monitor for potential deadlocks.
///
/// # Arguments
/// - `address`: The server address to connect to, in the format "ip:port".
/// - `client_key`: A unique identifier for the client, used for communication purposes.
///
/// # How it works
/// 1. The function first spawns a background thread that checks for deadlocks every 5 seconds.
///    If a deadlock is detected, the involved threads' IDs and backtraces are printed.
/// 2. The client attempts to establish a TCP connection with the server using the provided address.
/// 3. Once connected, the function enters a loop where it checks the `CLIENT_IS_RUNNING` global flag.
///    If the flag is set to false, the client will shut down.
/// 4. Inside the loop, the function retrieves the list of scheduled commands (up_schedule) to be sent
///    to the server. If there are no commands in the schedule, the client sends a ping to the server
///    and then waits for a short duration before checking again.
/// 5. For each command in the schedule, the client sends the command to the server and waits for a response.
///    The received response is then processed and scheduled for further handling.
///
/// # Notes
/// - The function uses `parking_lot::deadlock::check_deadlock()` to detect potential deadlocks.
/// - The client sends a ping to the server when there are no commands in the schedule.
/// - The client will wait for 200 milliseconds between retries if a command's response is not received.
/// - The client will continue to check for scheduled commands as long as `CLIENT_IS_RUNNING` is true.
pub async fn initialize_client(address: String, shutdown: Arc<Notify>) -> Option<String> {
    // Create a global Mutex for demonstration

    // Spawn a thread to periodically check for deadlocks
    thread::spawn(|| async {
        loop {
            tokio::time::sleep(Duration::from_secs(5)); // Check every 5 seconds
            let deadlocks = parking_lot::deadlock::check_deadlock();
            if deadlocks.is_empty() {
                continue;
            }

            let logger = acquire_logger!("Core");

            logger.debug(format!("{} deadlocks detected", deadlocks.len()));
            for (i, threads) in deadlocks.iter().enumerate() {
                logger.debug(format!("Deadlock #{}", i));
                for t in threads {
                    logger.debug(format!("Thread Id {:?}", t.thread_id()));
                    logger.debug(format!("{:?}", t.backtrace()));
                }
            }
        }
    });

    let logger = acquire_logger!("Core");

    // Here need to send the new handlers to host
    // then receive the host handlers

    logger.info(format!("Connected to {:?}!", &address).to_string());
    let client_key: String;

    logger.debug(format!("[CLIENT][GLOBAL][Try Lock] - CLIENT_ID"));
    {
        let c_uid = CLIENT_ID.lock();
        logger.debug(format!("[CLIENT][GLOBAL][Lock] - CLIENT_ID"));
        client_key = c_uid.clone()
    }
    logger.debug(format!("[CLIENT][GLOBAL][Release] - CLIENT_ID"));

    // -> ------------------------------------------------------------------------------------------------------------------------------------------------------------
    // -> Connect and split the stream in reader and writer:

    let address_clone: String = address.clone();
    let mut stream: TcpStream = first_connection(address_clone, &client_key).await;
    let (mut read_half, mut write_half) = stream.into_split();

    //> HeartBeat -> Sender
    //> UpCommand -> Sender

    // let (tx_to_dispatcher, mut rx_from_receiver) = mpsc::channel::<String>(32);
    // let (tx_to_sender, mut rx_from_dispatcher) = mpsc::channel::<String>(32);

    // TODO >>> Implement the shutdown logic usig shutdown rx -> based on the following:
    // if !CLIENT_IS_RUNNING.load(Ordering::SeqCst) {
    //     CLIENT_IS_SYNC.store(false, Ordering::SeqCst);
    //     logger.info("running is set to false, shutting down sender".into());
    //     break;
    // }

    // -> Make two channels:
    // >  - inbound:  host → client:
    let (tx_inbound, mut rx_inbound) = mpsc::channel::<String>(100);
    // >  - outbound: client → host:
    let (tx_outbound, mut rx_outbound) = mpsc::channel::<String>(100);

    // we use rx_inbound to receive from host
    // we use tx_outbound to send to host

    println!("Initializing rcv task!");

    let mut tasks = JoinSet::new();

    // -> Spawn the receiver task:
    let client_key_clone = client_key.clone();
    let recv_shutdown = shutdown.clone();
    let recv_task = tasks.spawn(async move {
        let logger = acquire_logger!("Core");
        select! {
            res = receiver(read_half, tx_inbound) => {
                if let Err(e) = res {
                    handle_stream_error(e, &client_key_clone);
                }
            }
            _ = recv_shutdown.notified() => {
                // Shutdown requested
                logger.info("Receiver task shutting down".to_string());
            }
        }
    });

    println!("Initializing snd task!");

    // -> Spawn the sender task:
    let client_key_clone = client_key.clone();
    let send_shutdown = shutdown.clone();
    let send_task = tasks.spawn(async move {
        let logger = acquire_logger!("Core");
        select! {
            res = sender(write_half, rx_outbound) => {
                if let Err(e) = res {
                    handle_stream_error(e, &client_key_clone);
                }
            }
            _ = send_shutdown.notified() => {
                logger.info("Sender task shutting down".to_string());
            }
        }
    });

    println!("Initializing heartbeat task!");

    // -> Periodic HeartBeat Async Task:
    let client_key_clone = client_key.clone();
    let tx_outbound_clone = tx_outbound.clone();
    let hb_shutdown = shutdown.clone();

    tasks.spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        interval.tick().await; // Fire the first tick immediately
        println!("Heartbeat task started");
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    // send heartbeat

                    let command_to_request: Command = create_special_command!(client_key, CommandMode::Function, "C206");
                    let command_string: String = serde_json::to_string(&command_to_request).unwrap();

                    if tx_outbound_clone.send(command_string).await.is_err() {
                        println!("Heartbeat: outgoing channel closed, stopping");
                        break;
                    }
                    println!("Heartbeat sent");
                },
                _ = hb_shutdown.notified() => {
                    println!("Heartbeat task shutting down");
                    break;
                }
            }
        }
    });

    // -> Auto Reconnection Logic
    // if !CLIENT_IS_CONNECTED.load(Ordering::SeqCst) {
    //     let stream = reconnect(address, &client_key).await;
    //     (read_half, write_half) = stream.into_split();
    // }

    //

    // Client Buffer Retriever
    // loop {
    // TODO >>> activelly receive from receiver (rx_inbound), and retrieve from buffer up to send to host up commands retrieved, throught (tx_outbound)
    // let command: Command = serde_json::from_str(&buffer_string).unwrap();
    // println!("Data received: {:?}\n", command);
    // logger.debug(format!("Received: {:?}", command));
    // return Ok(Response::Command(command));
    // }

    // -> ------------------------------------------------------------------------------------------------------------------------------------------------------------

    println!("Initializing buffer up retriever!");

    let tx_outbound_loader_clone = tx_outbound.clone();

    // -> Connection loop:
    let delay: u64 = 30u64;
    let logger = acquire_logger!("Core");
    let loader_shutdown = shutdown.clone();
    tasks.spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(50));
        interval.tick().await; // Fire the first tick immediately
        loop {
            select! { // TODO >>> Maybe use a tokio tick to execute it from time to time
                _ = loader_shutdown.notified() => {
                    logger.info("Shutdown signal received — exiting connection loop".to_string());
                    break;
                }
                _ = interval.tick() => {

                    if !CLIENT_IS_RUNNING.load(Ordering::SeqCst) {
                        CLIENT_IS_SYNC.store(false, Ordering::SeqCst);
                        logger.info(format!("running is set to false, shutdown socket client main process!"));
                        continue // Break!
                    }

                    // TODO >>> If client loses the connection maybe break or give some sign here, just try to reconnect here inside will not work
                    //> we will need another solution maybe send a signal or activate a task to try to reconnect externaly

                    let mut up_schedule = match enhanced_buffer::buffer_up_manager::buffer_up_list_schedule() {
                        Ok(ups) => ups,
                        Err(e) => panic!("{:?}", e),
                    };

                    let up_schdule_len = up_schedule.len();
                    if up_schdule_len == 0 {
                        // TODO >>> Instead of looping to find data to send, when have the IPCNS working start to use tx mpsc reactive activation to send data
                        // logger.debug(format!("Nothing in schedule to send to host, skipping!"));
                        continue // Do Not Break!
                    }

                    up_schedule.retain(|c| !COMMANDS_SENT_WAITING_RESPONSE.contains_key(&c.parity_id));

                    // TODO >>> Verify the latency of the command, if the command take too much time to generate a confiramtion something is wrong in the host.

                    if up_schdule_len > 1 {
                        logger.debug(format!("Find: {:?} command in schedule", 1));
                    } else {
                        logger.debug(format!("Find: {:?} commands in schedule", up_schdule_len));
                    }

                    logger.debug(format!("Start to process it!"));

                    let mut index: u32 = 0u32;
                    for up_command in up_schedule {
                        logger.debug(format!("processing command: {}", index));

                        if !CLIENT_IS_RUNNING.load(Ordering::SeqCst) {
                            CLIENT_IS_SYNC.store(false, Ordering::SeqCst);
                            logger.info(format!("running is set to false, shutdown socket client main process!"));
                            continue // Break!
                        }

                        let command_to_request: Command = match Command::from_up_command(&up_command) {
                            Ok(c) => c,
                            Err(error) => {
                                match error {
                                    CommandError::InvalidCommand(e) => {
                                        logger.exception(format!("Command: {:?} gives an exception when converting to command, the error was: \n{:?}", up_command, e));
                                    },
                                    CommandError::DeserializationError(e) => {
                                        logger.exception(format!("Command: {:?} gives and exception when converting to command, the error was: \n{:?}", up_command, e));
                                    },
                                    CommandError::InvalidResponse(e) => {
                                        logger.exception(format!("Command: {:?} have a InvalidResponse detected when converting to command, the error was: \n{:?}", up_command, e));
                                    },
                                    CommandError::NotAJsonObject => {
                                        logger.exception(format!("Command: {:?} Isn't a valid json command to be deserialized, verify if it is a object!", up_command));
                                    },
                                }

                                index = index + 1;
                                continue // Do Not Break!
                            },
                        };

                        COMMANDS_SENT_WAITING_RESPONSE.insert(command_to_request.parity_id.clone(), Instant::now());
                        let command_response_json: String = json!(&command_to_request).to_string();
                        match tx_outbound_loader_clone.send(command_response_json).await {
                            Ok(_) => {},
                            Err(e) => {
                                panic!("Error trying to send a command response json to the tx_outbound (Sender) channel, the error was: {:?}", e);
                            },
                        }

                        // -> Increment the count of the index in the queue in order to process the next.
                        index = index + 1;
                        continue // Do Not Break!
                    }

                    logger.debug(format!("End schedule data, so skipping >>>"));

                    continue // Do Not Break!
                }
            }
        }
    });

    while let Some(res) = tasks.join_next().await {
        match res {
            Ok(_) => {
                // A task ended normally (either by shutdown or finishing its work)
            },
            Err(join_err) if join_err.is_panic() => {
                // One of our tasks panicked!  Trigger the global shutdown.
                println!("⚠️  Detected panic in a task, notifying everyone to shut down…");
                shutdown.notify_waiters();
            },
            Err(join_err) => {
                // Shouldn't happen in normal usage, but worth logging
                eprintln!("task aborted: {}", join_err);
            },
        }
    }

    shutdown.notify_waiters();

    return None;
}
