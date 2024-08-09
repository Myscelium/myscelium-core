use crate::common::enhanced_buffer;
use crate::common::enhanced_buffer::buffer_down_manager::DownCommand;
use crate::common::enhanced_buffer::utilities::{Command, CommandError, CommandInstructions, CommandMode, CommandOrigin, CommandStatus, CommandTarget, CommandType};

use super::client_logger::log_handler::Logger;

use crate::{CLIENT_IS_SYNC, CLIENT_NODE_CONFIGS, MEDIAN_CON_RESP_TIME};

use indexmap::IndexMap;
use serde_json::json;
use serde_json::{from_str, Value};

use std::collections::HashMap;
use std::io::{self, Write};
use std::io::{ErrorKind, Read};
use std::net::TcpStream;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

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

#[derive(Debug)]
enum Response {
    Command(Command),
    None,
}

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
fn verify_connection(stream: &mut TcpStream, client_key: &String) -> bool {
    let logger = acquire_logger!("Core");

    let command = create_special_command!(client_key.clone(), CommandMode::Function, "C202");

    println!("\nTry to verify connection!");

    {
        let command_response_json = json!(command).to_string();
        let data_size = command_response_json.len() as u32;
        let size_buffer = data_size.to_be_bytes();

        // Send the size of the data
        match stream.write(&size_buffer) {
            Ok(_) => {},
            Err(e) => {
                println!("Error sending data lenght to client: {:?}, the error was:  {:?}", command.client_key, e);
            },
        };

        println!("Data lenght: {:?}", data_size);

        // Send the actual data
        match stream.write(command_response_json.as_bytes()) {
            Ok(_) => {},
            Err(e) => {
                println!("Error sending data to host: {:?} the error was: {:?}", command.client_key, e);
            },
        };

        println!("Connection verification sended!");
    }

    let mut size_buffer = [0; 4];

    // Read the size of the incoming data
    let data_size = match stream.read_exact(&mut size_buffer) {
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

    let buffer_string = match stream.read_exact(&mut data_buffer) {
        Ok(_) => String::from_utf8_lossy(&data_buffer).trim_end_matches(|c| c == '\n' || c == '\r' || c == '\0').to_string(),
        Err(e) => {
            eprintln!("Failed to read from the stream: {:?}", e);
            // Handle the error, e.g., by returning from the function or taking corrective action
            return false; // or handle differently
        },
    };

    println!("Confirmation data decoded!");

    let command: Command = serde_json::from_str(&buffer_string).unwrap();

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
fn send(stream: &mut TcpStream, command: &Command) -> Result<Response, StreamError> {
    let logger = acquire_logger!("Core");

    println!("Sending: {:?}", command);

    // {
    //     let conn: bool = verify_connection(stream, &command.client_key);
    //     if !conn {
    //         logger.info(format!("Not connected!"));
    //         return Response::None;
    //     }
    // }

    {
        let command_response_json = json!(command).to_string();
        let data_size = command_response_json.len() as u32;
        let size_buffer = data_size.to_be_bytes();

        // Send the size of the data
        match stream.write(&size_buffer) {
            Ok(_) => {},
            Err(e) => {
                return Err(StreamError::WriteSizeError(e));
            },
        };

        println!("Data lenght: {:?}", size_buffer);

        // Send the actual data
        match stream.write(command_response_json.as_bytes()) {
            Ok(_) => {},
            Err(e) => {
                return Err(StreamError::WriteError(e));
            },
        };

        println!("Data sended!")
    }

    let mut size_buffer = [0; 4];

    // Read the size of the incoming data
    let data_size = match stream.read_exact(&mut size_buffer) {
        Ok(_) => u32::from_be_bytes(size_buffer) as usize,
        Err(e) => {
            return Err(StreamError::ReadSizeError(e));
        },
    };

    println!("Receive incomming data lenght: {}", data_size);

    if data_size > MAX_DATA_SIZE {
        logger.exception(format!("Received data size does not match expected size: {} max bytes expected, {} bytes received", MAX_DATA_SIZE, data_size));
        return Err(StreamError::ConnectionClosed); // TODO >>> Close connection or handle appropriately
    }

    println!("Data isn't greather than leght limit!");

    // Allocate a buffer of the appropriate size
    let mut data_buffer = vec![0; data_size];

    let buffer_string = match stream.read_exact(&mut data_buffer) {
        Ok(_) => String::from_utf8_lossy(&data_buffer).trim_end_matches(|c| c == '\n' || c == '\r' || c == '\0').to_string(),
        Err(e) => {
            return Err(StreamError::ReadDataError(e));
        },
    };
    println!("Received binary data");

    let command: Command = serde_json::from_str(&buffer_string).unwrap();

    println!("Data received: {:?}\n", command);

    logger.debug(format!("Received: {:?}", command));

    return Ok(Response::Command(command));
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
pub fn send_ping(stream: &mut TcpStream, client_key: &String) -> Result<Option<DownCommand>, StreamError> {
    let logger = acquire_logger!("[CLIENT][SOCKET]");

    let command_to_request = create_special_command!(client_key, CommandMode::Function, "C206");

    println!("Create C206 ping request: {:?}", command_to_request);

    // Send command and measure time
    let start = Instant::now();
    let received = send(stream, &command_to_request)?;
    let duration = start.elapsed();

    // Retry mechanism for lock acquisition
    loop {
        match MEDIAN_CON_RESP_TIME.try_lock() {
            Some(mut guard) => {
                if guard.len() >= 10 {
                    guard.remove(0);
                }
                guard.push(duration.as_nanos() as f64);
                break; // Exit loop after successful lock and update
            },
            None => {
                // If lock can't be acquired, sleep for a short duration
                thread::sleep(Duration::from_millis(50));
            },
        }
    }

    logger.debug(format!("Received response: {:?}", received));

    match received {
        Response::Command(c) => {
            match c.command.status {
                CommandStatus::Failure => {
                    logger.exception(format!("\nAn error occurred in host, the error was: {}\n", c.command.message));
                    CLIENT_IS_RUNNING.store(false, Ordering::SeqCst);
                    CLIENT_IS_CONNECTED.store(false, Ordering::SeqCst);
                    return Ok(None);
                },
                CommandStatus::Success => {},
            }

            match c.command_type() {
                CommandType::SpecialFunction => {
                    if c.command.actf == "C207" {
                        logger.debug(format!("Receive ping response pong conf!"));
                        return Ok(None);
                    };
                    if c.parity_id != "itisaspecialcase" {
                        if c.command.actf == "C210".to_string() {
                            logger.debug(format!("Received Confirmation! Removing command {} of client: {} from buffer up", c.parity_id, c.client_key));
                            enhanced_buffer::buffer_up_manager::buffer_up_remove_schedule_by_parity_id(&c.client_key, &c.parity_id);
                            return Ok(None);
                        }
                    }
                },
                _ => {},
            }
            let down_command: DownCommand = DownCommand::from_command(c);
            return Ok(Some(down_command));
        },
        Response::None => {
            return Ok(None);
        },
    }
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
pub fn initialize_client(address: String) -> Option<String> {
    // Create a global Mutex for demonstration

    // Spawn a thread to periodically check for deadlocks
    thread::spawn(|| {
        loop {
            thread::sleep(Duration::from_secs(5)); // Check every 5 seconds
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

    fn connect(address: String) -> Option<TcpStream> {
        let logger = acquire_logger!("Core");

        let mut stream = match TcpStream::connect(address.clone()) {
            Ok(s) => s,
            Err(e) => match e.kind() {
                ErrorKind::ConnectionRefused => {
                    logger.debug(format!("Can't connect in host!"));
                    return None;
                },
                _ => {
                    logger.debug(format!("Unhandled error: {}", e));
                    panic!("Unhandled error: {}", e)
                },
            },
        };

        stream.set_read_timeout(Some(Duration::new(15, 0))).unwrap();
        Some(stream)
    }

    // Here need to send the new handlers to host
    // then receive the host handlers

    logger.info(format!("Connected to {:?}!", &address).to_string());
    thread::sleep(Duration::from_millis(5));
    let client_key: String;

    logger.debug(format!("[CLIENT][GLOBAL][Try Lock] - CLIENT_ID"));

    {
        let c_uid = CLIENT_ID.lock();
        logger.debug(format!("[CLIENT][GLOBAL][Lock] - CLIENT_ID"));

        client_key = c_uid.clone()
    }

    logger.debug(format!("[CLIENT][GLOBAL][Release] - CLIENT_ID"));

    let mut stream: TcpStream;
    let mut last_attempt_time: Instant = Instant::now() - Duration::from_secs(30);
    loop {
        let now = Instant::now();
        if now.duration_since(last_attempt_time) >= Duration::from_secs(30) {
            // try to connect again
            if let Some(st) = connect(address.clone()) {
                stream = st;
                if verify_connection(&mut stream, &client_key) {
                    CLIENT_IS_CONNECTED.store(true, Ordering::SeqCst);
                    break;
                }
            }

            // Update last attempt time
            last_attempt_time = now
        } else {
            let dif: Duration = last_attempt_time - now;
            println!("Trying to connect again in: {} secs", dif.as_secs());
        }
    }

    loop {
        if !CLIENT_IS_RUNNING.load(Ordering::SeqCst) {
            CLIENT_IS_SYNC.store(false, Ordering::SeqCst);
            logger.info(format!("running is set to false, shutdown socket client main process!"));
            break;
        }

        if !CLIENT_IS_CONNECTED.load(Ordering::SeqCst) {
            let now = Instant::now();
            if now.duration_since(last_attempt_time) >= Duration::from_secs(30) {
                // try to connect again
                if let Some(st) = connect(address.clone()) {
                    stream = st;
                    if verify_connection(&mut stream, &client_key) {
                        CLIENT_IS_CONNECTED.store(true, Ordering::SeqCst);
                        continue;
                    }
                }
                // Update last attempt time
                last_attempt_time = now
            } else {
                let dif: Duration = (last_attempt_time + Duration::from_secs(30)) - now;
                println!("Trying to connect again in: {} secs", dif.as_secs());
            }
            thread::sleep(Duration::from_secs(1u64));
            continue;
        }

        let up_schedule = enhanced_buffer::buffer_up_manager::buffer_up_list_schedule();

        let up_schdule_len = up_schedule.len();

        if !(up_schdule_len > 0) {
            let option_down_command: Option<DownCommand> = match send_ping(&mut stream, &client_key) {
                Ok(d) => d,
                Err(e) => {
                    logger.exception("A exception occurred!".to_string());
                    CLIENT_IS_CONNECTED.store(false, Ordering::SeqCst);
                    CLIENT_IS_SYNC.store(false, Ordering::SeqCst);
                    match e {
                        StreamError::ConnectionClosed => {
                            logger.exception(format!("[CLIENT][SOCKET][CLOSE CONNECTION] - {}", &client_key));
                            continue;
                        },
                        StreamError::WriteError(e) => {
                            logger.exception(format!("[CLIENT][SOCKET][WRITE ERROR] - {:?}", e));
                            logger.exception(format!("[CLIENT][SOCKET][CLOSE CONNECTION] - {}", &client_key));
                            match e.kind() {
                                io::ErrorKind::ConnectionReset => continue,
                                _ => {
                                    CLIENT_IS_RUNNING.store(false, Ordering::SeqCst);
                                    println!("Other error occurred")
                                },
                            }
                            break;
                        },
                        StreamError::WriteSizeError(e) => {
                            logger.exception(format!("[CLIENT][SOCKET][WRITE SIZE ERROR] - {:?}", e));
                            logger.exception(format!("[CLIENT][SOCKET][CLOSE CONNECTION] - {}", &client_key));
                            match e.kind() {
                                io::ErrorKind::ConnectionReset => continue,
                                _ => {
                                    CLIENT_IS_RUNNING.store(false, Ordering::SeqCst);
                                    println!("Other error occurred")
                                },
                            }
                            break;
                        },
                        StreamError::ReadSizeError(e) => {
                            logger.exception(format!("[CLIENT][SOCKET][READ SIZE ERROR] - {:?}", e));
                            logger.exception(format!("[CLIENT][SOCKET][CLOSE CONNECTION] - {}", &client_key));
                            match e.kind() {
                                io::ErrorKind::ConnectionReset => continue,
                                _ => {
                                    CLIENT_IS_RUNNING.store(false, Ordering::SeqCst);
                                    println!("Other error occurred")
                                },
                            }
                            break;
                        },
                        StreamError::ReadDataError(e) => {
                            logger.exception(format!("[CLIENT][SOCKET][READ DATA ERROR] - {:?}", e));
                            logger.exception(format!("[CLIENT][SOCKET][CLOSE CONNECTION] - {}", &client_key));
                            match e.kind() {
                                io::ErrorKind::ConnectionReset => continue,
                                _ => {
                                    CLIENT_IS_RUNNING.store(false, Ordering::SeqCst);
                                    println!("Other error occurred")
                                },
                            }
                            break;
                        },
                    };
                },
            };

            logger.debug(format!("Nothing in schedule to send to host, so sending ping!"));
            if let Some(down_command) = option_down_command {
                enhanced_buffer::buffer_down_manager::buffer_down_schedule(&down_command);
            } else {
                logger.debug(format!("[Socket] - No command received in ping, skipping.."));
            }

            thread::sleep(Duration::from_millis(10));
            continue;
        }

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
                break;
            }

            let command_to_request = match Command::from_up_command(&up_command) {
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
                    continue;
                },
            };

            loop {
                logger.debug(format!("Sending to host: {:?}", &command_to_request));

                let received: Response;

                if !CLIENT_IS_RUNNING.load(Ordering::SeqCst) {
                    logger.info(format!("running is set to false, shutdown socket client main process!"));
                    break;
                }

                {
                    received = match send(&mut stream, &command_to_request) {
                        Ok(r) => r,
                        Err(e) => {
                            CLIENT_IS_CONNECTED.store(false, Ordering::SeqCst);
                            CLIENT_IS_SYNC.store(false, Ordering::SeqCst);
                            match e {
                                StreamError::ConnectionClosed => {
                                    logger.debug(format!("[CLIENT][SOCKET][CLOSE CONNECTION] - {}", &client_key));
                                    continue;
                                },
                                StreamError::WriteError(e) => {
                                    logger.debug(format!("[CLIENT][SOCKET][WRITE ERROR] - {:?}", e));
                                    logger.debug(format!("[CLIENT][SOCKET][CLOSE CONNECTION] - {}", &client_key));
                                    match e.kind() {
                                        io::ErrorKind::ConnectionReset => continue,
                                        _ => {
                                            CLIENT_IS_RUNNING.store(false, Ordering::SeqCst);
                                            println!("Other error occurred")
                                        },
                                    }
                                    break;
                                },
                                StreamError::WriteSizeError(e) => {
                                    logger.debug(format!("[CLIENT][SOCKET][WRITE SIZE ERROR] - {:?}", e));
                                    logger.debug(format!("[CLIENT][SOCKET][CLOSE CONNECTION] - {}", &client_key));
                                    match e.kind() {
                                        io::ErrorKind::ConnectionReset => continue,
                                        _ => {
                                            CLIENT_IS_RUNNING.store(false, Ordering::SeqCst);
                                            println!("Other error occurred")
                                        },
                                    }
                                    break;
                                },
                                StreamError::ReadSizeError(e) => {
                                    logger.debug(format!("[CLIENT][SOCKET][READ SIZE ERROR] - {:?}", e));
                                    logger.debug(format!("[CLIENT][SOCKET][CLOSE CONNECTION] - {}", &client_key));
                                    match e.kind() {
                                        io::ErrorKind::ConnectionReset => continue,
                                        _ => {
                                            CLIENT_IS_RUNNING.store(false, Ordering::SeqCst);
                                            println!("Other error occurred")
                                        },
                                    }
                                    break;
                                },
                                StreamError::ReadDataError(e) => {
                                    logger.debug(format!("[CLIENT][SOCKET][READ DATA ERROR] - {:?}", e));
                                    logger.debug(format!("[CLIENT][SOCKET][CLOSE CONNECTION] - {}", &client_key));
                                    match e.kind() {
                                        io::ErrorKind::ConnectionReset => continue,
                                        _ => {
                                            CLIENT_IS_RUNNING.store(false, Ordering::SeqCst);
                                            println!("Other error occurred")
                                        },
                                    }
                                    break;
                                },
                            }
                        },
                    };
                }

                match received {
                    Response::Command(c) => {
                        match c.command.status {
                            CommandStatus::Failure => {
                                logger.exception(format!("\nAn error occurred in host, the error was: {}\n", c.command.message));
                                enhanced_buffer::buffer_up_manager::buffer_up_remove_schedule_by_parity_id(&c.client_key, &c.parity_id);
                                CLIENT_IS_RUNNING.store(false, Ordering::SeqCst);
                                CLIENT_IS_CONNECTED.store(false, Ordering::SeqCst);
                                CLIENT_IS_SYNC.store(false, Ordering::SeqCst);
                                break;
                            },
                            CommandStatus::Success => {},
                        }

                        match c.command_type() {
                            CommandType::ExternalFunction => {
                                let down_command = DownCommand::from_command(c);
                                logger.debug(format!("[Socket Client] - Receives Data.. : {:?}", down_command));
                                enhanced_buffer::buffer_down_manager::buffer_down_schedule(&down_command);
                                index = index + 1;
                                break;
                            },
                            CommandType::DirectFunction => {
                                // TODO >>> Need to actualize this to the new patter like Response handler to redirect works as intended!
                                // > Also we can use a similar system to sync multiple hosts
                                logger.info(format!("[Socket Client] - Received a direct function!:\n {:?}", c.command.actf));
                                let down_command = DownCommand::from_command(c);
                                enhanced_buffer::buffer_down_manager::buffer_down_schedule(&down_command);
                                index = index + 1;
                                break;
                            },
                            CommandType::SpecialFunction => {
                                if c.parity_id != "itisaspecialcase" {
                                    if c.command.actf == "C210".to_string() {
                                        logger.info(format!("Received Confirmation! Removing command {} of client: {} from buffer up", c.parity_id, c.client_key));
                                        enhanced_buffer::buffer_up_manager::buffer_up_remove_schedule_by_parity_id(&c.client_key, &c.parity_id);
                                        break;
                                    }
                                }

                                if c.command.actf == "C207".to_string() {
                                    logger.debug(format!("Receive ping confirmation"));
                                    break;
                                }

                                logger.debug(format!("Received a unknow special function"));
                                break;
                            },
                        }
                    },
                    Response::None => {
                        break;
                    },
                }

                logger.debug(format!("Invalid command!"));
                break;
            }

            index = index + 1;
            continue;
        }

        logger.debug(format!("End schedule data, so skipping >>>"));

        continue;
    }

    return None;
}
