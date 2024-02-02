#[allow(unused_imports)]
#[allow(unused_extern_crates)]
mod common;

mod socket_client;
mod socket_host;

use common::structs::available_commands::Node;

use lazy_static::lazy_static;
use serde_json::Value;

use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use parking_lot::Mutex;

extern crate chrono;
use crate::common::structs::available_commands::NetworkMap;
use crate::socket_client::states_manager::manager::ClientState;

use crate::common::structs::callbacks::{CallbackClosure, MyCallbacks};

lazy_static! {

    // CLIENT
    pub static ref CLIENT_IS_RUNNING: Arc<AtomicBool> = Arc::new(AtomicBool::new(true));
    pub static ref CLIENT_IS_SYNC: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));
    pub static ref CLIENT_NODE_KEY: Arc<Mutex<String>> = Arc::new(Mutex::new("".to_string()));
    pub static ref CLIENT_NODE_NAME: Arc<Mutex<String>> = Arc::new(Mutex::new("".to_string()));
    pub static ref CLIENT_LOG_LEVEL: Arc<Mutex<String>> = Arc::new(Mutex::new("".to_string()));
    pub static ref CLIENT_IS_READY: Arc<AtomicBool> = Arc::new(AtomicBool::new(false)); // TODO >>> Finish the impl of this
    pub static ref CLIENT_NODE_CONFIGS: Arc<Mutex<Node>> = Arc::new(Mutex::new(Node::empty_node()));
    pub static ref CLIENT_STATE_MANAGER: Arc<Mutex<ClientState>> = Arc::new(Mutex::new(ClientState::empty()));
    pub static ref CLIENT_CALLBACK_PATTERNS: MyCallbacks = MyCallbacks::new();

    // HOST:
    pub static ref HOST_IS_RUNNING: Arc<AtomicBool> = Arc::new(AtomicBool::new(true));
    pub static ref HOST_NODE_NAME: Arc<Mutex<String>> = Arc::new(Mutex::new("".to_string()));
    pub static ref HOST_LOG_LEVEL: Arc<Mutex<String>> = Arc::new(Mutex::new("".to_string()));
    pub static ref HOST_IS_READY: Arc<AtomicBool> = Arc::new(AtomicBool::new(false)); // TODO >>> Finish the impl of this
    pub static ref HOST_COMMAND_PATTERNS: Arc<Mutex<NetworkMap>> = Arc::new(Mutex::new(NetworkMap::new(Vec::new())));
    pub static ref HOST_CALLBACK_PATTERNS: MyCallbacks = MyCallbacks::new();

}

fn main() {}

// -------------------------------------------------------------------------------------------------------------------------------------------------------------
// -> CLIENT:

use crate::common::enhanced_buffer::utilities::{CommandInstructions, CommandType};
use crate::common::structs::available_commands::{HandlerStatus, NodeHandler, NodeStatus, NodeVersion, VersionIndentifier};
use crate::socket_client::client_logger::log_handler::{initialize_client_logs_database_dir, set_client_log_level};
use crate::socket_client::states_manager::manager::{inialize_client_status_table_table, StateManagerError};
use std::collections::HashMap;
use std::process::Command;
use std::sync::atomic::Ordering;
use std::thread;
use std::time::Duration;

// -> Socket Client main-points:

// use crate::common::functions::callbacks::extract_arg_types;
use crate::socket_client::scheduler::{self, schedule};
use crate::socket_client::socket_client::get_available_handlers_registered;
use crate::socket_client::socket_client::{initialize_client, initialize_client_buffer};
use crate::socket_client::transposer::{initialize_socket_client_transposer, set_socket_client_transposer_callbacks, set_socket_client_transposer_workers_num};

pub fn set_socket_client_transposer_num_of_workers(n_workers: u32) {
    set_socket_client_transposer_workers_num(n_workers);
    return;
}

/// Stops the socket client.
///
/// # Behavior
///
/// Sets the global `CLIENT_IS_RUNNING` atomic flag to `false`.
///
fn stop_socket_client() {
    CLIENT_IS_RUNNING.store(false, Ordering::SeqCst);
}

pub fn initialize_client_buffer_tables(path: &String) {
    initialize_client_logs_database_dir(path.clone());
    initialize_client_buffer(path.clone());
    inialize_client_status_table_table(path.clone());

    return;
}

#[derive(Debug, Clone)]
enum ResultType {
    Empty,
    Map(HashMap<String, String>),
    Error(String),
}

pub fn is_target_ready(node_key: String) -> bool {
    let client_status = match ClientState::load_from_storage() {
        Ok(c) => c,
        Err(_) => {
            return false;
        },
    };

    if let Some(net_map) = client_status.network_map {
        let mut net_map = net_map;
        {
            match net_map.target_is_reachable(&node_key) {
                Ok(reachable) => {
                    if !reachable {
                        return false;
                    }
                },
                Err(_) => {
                    return false;
                },
            };
        }
        {
            match net_map.target_is_ready(&node_key) {
                Ok(redy) => {
                    if !redy {
                        return false;
                    }
                },
                Err(_) => {
                    return false;
                },
            };
        }
    } else {
        return false;
    }

    return true;
}

pub fn is_client_ready() -> bool {
    let client_status = match ClientState::load_from_storage() {
        Ok(c) => c,
        Err(_) => return false,
    };

    //if !client_status.is_fully_initialized() {
    //    return false;
    //}

    if let Some(sync) = client_status.is_sync {
        if !sync {
            return false;
        };
    } else {
        return false;
    }

    if let Some(ready) = client_status.is_ready {
        if !ready {
            return false;
        }
    } else {
        return false;
    }

    return true;
}

pub enum ClientError {
    ClientIsNotRunning,
    ClientNotFullyInitialized,
    NotAbleToReadClientStates,
}

pub fn client_send_hashmap(command: HashMap<String, String>, priority: u8) -> Result<(), ClientError> {
    if !CLIENT_IS_RUNNING.load(Ordering::SeqCst) {
        println!("Error, client isn't running, pls run the client before try to send something!");
        return Err(ClientError::ClientIsNotRunning);
    }

    let command_instructions = CommandInstructions::from_string_hashmap(command).unwrap();

    let _ = match schedule(command_instructions, priority) {
        Ok(o) => o,
        Err(e) => match e {
            scheduler::SchedulingError::CantReadStates => {
                return Err(ClientError::NotAbleToReadClientStates);
            },
            scheduler::SchedulingError::ClientIsntFullyInitialized => {
                return Err(ClientError::ClientNotFullyInitialized);
            },
        },
    };

    Ok(())
}

pub fn client_send(command: CommandInstructions, priority: u8) -> Result<(), ClientError> {
    if !CLIENT_IS_RUNNING.load(Ordering::SeqCst) {
        println!("Error, client isn't running, pls run the client before try to send something!");
        return Err(ClientError::ClientIsNotRunning);
    }

    let _ = match schedule(command, priority) {
        Ok(o) => o,
        Err(e) => match e {
            scheduler::SchedulingError::CantReadStates => {
                return Err(ClientError::NotAbleToReadClientStates);
            },
            scheduler::SchedulingError::ClientIsntFullyInitialized => {
                return Err(ClientError::ClientNotFullyInitialized);
            },
        },
    };

    Ok(())
}

/// Sets the log level for the client.
///
/// # Parameters
///
/// - `log_level`: The desired log level as a string.
///
/// # Behavior
///
/// Updates the logging level of the client.
pub fn set_socket_client_log_level(log_level: &String) {
    set_client_log_level(log_level.clone());
    return;
}

pub fn get_socket_client_available_handlers() -> HashMap<String, Value> {
    get_available_handlers_registered()
}

pub fn set_client_callbacks(callbacks: HashMap<String, Box<CallbackClosure>>) {
    for (key, callback) in callbacks {
        set_socket_client_transposer_callbacks(key, callback)
    }
}

// fn concatenate_strings(args: Vec<Box<dyn Any + 'static>>) -> Box<dyn Any> {
//     let mut result = String::new();
//     for arg in args {
//         if let Some(string_arg) = arg.downcast_ref::<String>() {
//             result.push_str(string_arg);
//         }
//     }
//     Box::new(result) as Box<dyn Any>
// }

pub fn get_client_state() -> bool {
    if CLIENT_IS_RUNNING.load(Ordering::SeqCst) {
        true
    } else {
        false
    }
}

pub fn set_client_key(client_key: String) {
    socket_client::socket_client::set_client_uid(client_key.clone());
    {
        let mut key = CLIENT_NODE_KEY.lock();
        *key = client_key.clone();
    }
}

/// Initializes the socket client, sets up deadlock detection, and starts the main processing loop.
///
/// This function sets up the socket client to communicate with a server and starts the main loop
/// for processing commands and callbacks. It also spawns a thread to periodically check for deadlocks.
///
/// # Parameters
///
/// - `py`: Python interpreter instance.
/// - `ip`: IP address of the server.
/// - `port`: Port number of the server.
/// - `client_id`: A unique identifier for the client.
///
/// # Behavior
///
/// - Sets up a thread to periodically detect deadlocks.
/// - Initializes global client state.
/// - Spawns a thread to handle `Ctrl+C` and gracefully shut down the client.
/// - Initializes the socket client connection.
/// - Requests available commands from the host.
/// - Enters the main command processing loop.
///
/// # Python Binding
///
/// This function is exposed to Python and can be called from a Python script.
pub fn initialize_socket_client(ip: String, port: i32, client_key: String, client_name: String) {
    // Spawn a thread to periodically check for deadlocks
    thread::spawn(|| {
        loop {
            let deadlocks = parking_lot::deadlock::check_deadlock();
            if deadlocks.is_empty() {
                thread::sleep(Duration::from_millis(200)); // Check every 200 millis
                continue;
            }

            println!("{} deadlocks detected", deadlocks.len());
            for (i, threads) in deadlocks.iter().enumerate() {
                println!("Deadlock #{}", i);
                for t in threads {
                    println!("Thread Id {:?}", t.thread_id());
                    println!("{:?}", t.backtrace());
                }
            }
        }
    });

    // -> SET CLIENT NAME IN CLIENT STATE MANAGER MEMORY SO WHEN THE CALLBACKS BE REGISTRED IT CAN
    // BE APLIED
    {
        let mut name = CLIENT_NODE_NAME.lock();
        *name = client_name.clone();
        let mut client_states = ClientState::load_from_storage().unwrap();
        client_states.name = Some(name.clone());
        client_states.update_schedule_with_this().unwrap();
    }

    CLIENT_IS_RUNNING.store(true, Ordering::SeqCst);

    // let mut client_key: String = "".to_string();

    {
        let mut key = CLIENT_NODE_KEY.lock();
        *key = client_key.clone();
    }

    // let client_key_storage = CLIENT_ID;
    // smart_lock(&*client_key_storage, |key: &mut String| {
    //     *key = client_id.clone();
    // });

    let address = format!("{}:{}", ip, port);

    thread::spawn(|| {
        ctrlc::set_handler(move || {
            if CLIENT_IS_RUNNING.load(Ordering::SeqCst) {
                CLIENT_IS_RUNNING.store(false, Ordering::SeqCst);
                println!("\nreceived Ctrl+C!\n");
                stop_socket_client();
            }
        })
        .expect("Error setting Ctrl-C handler");

        initialize_client(address);

        println!("Socket host exited successfully!");

        CLIENT_IS_RUNNING.store(false, Ordering::SeqCst);
    });

    // scheduler::request_host_available_commands();

    loop {
        println!("➡️ Client status: {}", CLIENT_IS_RUNNING.load(Ordering::SeqCst));

        if !CLIENT_IS_RUNNING.load(Ordering::SeqCst) {
            println!("Stop the core!");
            break;
        }

        initialize_socket_client_transposer();
    }

    println!("Socket transposer exited successfully!");
}

// -------------------------------------------------------------------------------------------------------------------------------------------------------
// -> HOST

// use crate::common::enhanced_buffer::utilities::CommandType;
// use crate::common::functions::callbacks::extract_arg_types;
// use crate::common::functions::callbacks::translate_value_to_py;
use crate::socket_host::client_manager::manager::Client;
use crate::socket_host::client_manager::manager::{check_if_client_key_exists, clients_manager_initialize_table, set_host_clients_manager__pool_workers_num};
use crate::socket_host::host_logger::log_handler::{initialize_host_logs_database_dir, set_host_log_level};
use crate::socket_host::socket_host::{get_available_commands_registered, initialize_host};
use crate::socket_host::socket_host::{initialize_host_buffer, set_heartbeat_callback, set_max_conns};
use crate::socket_host::sync_controller::controller::{ClientStatusPoolError, Clients};
use crate::socket_host::transposer::{initialize_socket_host_transposer, set_socket_host_transposer_callbacks, set_socket_host_transposer_workers_num};

lazy_static! {
    pub static ref CLIENTS_SYNC_CONTROLLER: Arc<Mutex<Clients>> = Arc::new(Mutex::new(Clients::new()));
}

fn set_socket_host_transposer_num_of_workers(n_workers: u32) {
    set_socket_host_transposer_workers_num(n_workers);
    return;
}

fn set_socket_host_max_connections(n_max_conns: u32) {
    set_host_clients_manager__pool_workers_num(n_max_conns.clone());
    set_max_conns(n_max_conns);
    return;
}

fn initialize_host_buffer_tables(path: String) {
    initialize_host_logs_database_dir(path.clone());
    initialize_host_buffer(path.clone());
    clients_manager_initialize_table(path.clone());

    return;
}

fn set_socket_host_log_level(log_level: String) {
    set_host_log_level(log_level);
    return;
}

// pub fn registry_socket_host_client_heartbeat_contact_callback(commands: &PyList) -> PyResult<()> {
//     let mut callback_pattern = HashMap::new();

//     process_commands!(py, commands, callback_pattern);

//     set_heartbeat_callback(callback_pattern);

//     Ok(())
// }

pub fn get_socket_host_available_commands() -> HashMap<String, Value> {
    get_available_commands_registered()
}

// > --------------------------------------------------------------------------------------------------------
// > Client Management

// use crate::handle_client_error;

pub fn add_allowed_clients(new_allowed_clients_list: Vec<Client>) {
    for client_allowed in new_allowed_clients_list.iter() {
        if !check_if_client_key_exists(client_allowed.client_key.clone()) {
            client_allowed.save_into_db()
        }

        {
            let mut controller = CLIENTS_SYNC_CONTROLLER.lock();
            let _ = controller.add_new_client(client_allowed.client_key.clone().to_string(), 10);
            println!("\nSet clients sync controler to:\n{:?}\n", controller);
        }

        println!("Successfully created client: {} of key: {}", client_allowed.client_name, client_allowed.client_key)
    }
}

/// Removes all clients from the list of clients allowed to connect to the socket host.
///
/// This function clears the global list of clients that are permitted to connect to the socket host. After calling this function,
/// no client will be able to connect until new clients are added using either `set_socket_host_allowed_clients` or `registry_new_allowed_clients`.
///
/// # Parameters
///
/// - `allowed_client_list`: A Python list of dictionaries, same structure as `set_socket_host_allowed_clients`.
///
/// # Python Binding
/// This function is exposed to Python and can be called from a Python script.

fn remove_all_allowed_clients() {
    let _ = Client::delete_all();
}

// ->-------------------------------------------------------------------------------------------------------------------------------
// -> HOST CONTROLLERS

/// Stops the socket host.
///
/// This function sets the global `HOST_IS_RUNNING` flag to false, indicating that the socket host should stop running.
fn stop_socket_host() {
    HOST_IS_RUNNING.store(false, Ordering::SeqCst);
}

// TODO >>> DEVELOP A MECHANISM TO BE ABLE TO SET RUST FUNCTIONS AS CALLBACKS, THIS ALSO NEEDS TO BE PROCEDURALLY CREATABLE

pub fn setup_socket_host(buffer_path: &String, log_level: &String, n_workers: &u32, n_max_conns: &u32) {
    initialize_host_buffer_tables(buffer_path.clone());
    set_socket_host_log_level(log_level.clone());
    set_socket_host_transposer_num_of_workers(n_workers.clone());
    set_socket_host_max_connections(n_max_conns.clone());
}

/// Initializes and starts the socket host.
///
/// This function sets up the socket host and starts it, allowing it to accept incoming connections.
///
/// # Parameters
///
/// - `py`: The Python interpreter.
/// - `ip`: IP address for the socket host.
/// - `port`: Port for the socket host.
/// - `client_id`: ID of the client.
///
/// # Python Binding
///
/// This function is exposed to Python and can be called from a Python script.
pub fn initialize_socket_host(ip: String, port: i32, client_id: String) {
    // Create a global Mutex for demonstration
    let mutex1 = Mutex::new(0);
    let mutex2 = Mutex::new(0);

    // Spawn a thread to periodically check for deadlocks
    thread::spawn(|| {
        loop {
            thread::sleep(Duration::from_secs(5)); // Check every 5 seconds
            let deadlocks = parking_lot::deadlock::check_deadlock();
            if deadlocks.is_empty() {
                continue;
            }

            println!("{} deadlocks detected", deadlocks.len());
            for (i, threads) in deadlocks.iter().enumerate() {
                println!("Deadlock #{}", i);
                for t in threads {
                    println!("Thread Id {:?}", t.thread_id());
                    println!("{:?}", t.backtrace());
                }
            }
        }
    });

    let address = format!("{}:{}", ip, port);

    thread::spawn(|| {
        ctrlc::set_handler(move || {
            if HOST_IS_RUNNING.load(Ordering::SeqCst) {
                println!("\nreceived Ctrl+C!\n");
                stop_socket_host();
            }
        })
        .expect("Error setting Ctrl-C handler");

        initialize_host(address, client_id);
        println!("Socket host exited successfully!");
    });

    loop {
        initialize_socket_host_transposer();

        if !HOST_IS_RUNNING.load(Ordering::SeqCst) {
            println!("Stop the core!");
            thread::sleep(Duration::from_secs(7));
            break;
        }
    }

    println!("Socket transposer exited successfully!");
}
