#[allow(unused_imports)]
#[allow(unused_extern_crates)]
#[allow(warnings)]
#[allow(dead_code, unused_variables)]
#[allow(unused_results)]
#[allow(unused_variables)]
mod common;
#[allow(unused_imports)]
#[allow(unused_extern_crates)]
#[allow(warnings)]
#[allow(dead_code, unused_variables)]
#[allow(unused_results)]
#[allow(unused_variables)]
mod socket_client;
#[allow(unused_imports)]
#[allow(unused_extern_crates)]
#[allow(warnings)]
#[allow(dead_code, unused_variables)]
#[allow(unused_results)]
#[allow(unused_variables)]
mod socket_host;

use common::enhanced_buffer::utilities::CommandError;
use indexmap::IndexMap;
#[allow(unused_imports)]
#[allow(unused_extern_crates)]
#[deny(warnings)]
#[allow(dead_code, unused_variables)]
#[allow(unused_results)]
#[allow(unused_variables)]
use lazy_static::lazy_static;
use serde_json::Value;

use core::panic;
#[deny(non_snake_case)]
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::thread::Thread;

use parking_lot::Mutex;

extern crate chrono;
// use crate::socket_client::states_manager::manager::ClientState;

pub use crate::common::client_manager::manager::ClientError;
pub use crate::common::enhanced_buffer::utilities::Command;
use crate::common::structs::callbacks::{CallbackClosure, MyCallbacks};
use crate::socket_client::client_logger::log_handler::set_client_log_level;
pub use common::client_network_controller::availability_controller::AllowedNetWorkController;
pub use common::enhanced_buffer::utilities::CommandInstructions;
pub use common::enhanced_buffer::utilities::CommandType;
pub use common::structs::available_commands::{HandlerStatus, NetworkMap, Node, NodeHandler, NodeStatus, NodeVersion, VersionIndentifier};
pub use common::structs::callbacks_structure::Callback;
pub use common::structs::results_structs::ResultType;
pub use socket_client::states_manager::manager::{ClientState, StateManagerError};

// -> HOST
pub use crate::common::client_manager::manager::check_if_client_key_exists;
pub use crate::common::client_manager::manager::registry_new_client;
pub use crate::common::client_manager::manager::Client;
pub use crate::socket_host::sync_controller::controller::{ClientStatusPoolError, Clients};
pub use crate::socket_host::transposer_functions::handle_direct_function::ProcessResult;
pub use socket_host::socket_host::set_heartbeat_callback;

lazy_static! {

    // Crate

    pub static ref CLIENT_VERSION: NodeVersion = NodeVersion::cast_version(1, 3, 0, VersionIndentifier::ReleaseCandidate);
    pub static ref HOST_VERSION: NodeVersion = NodeVersion::cast_version(1, 3, 0, VersionIndentifier::ReleaseCandidate);


    // CLIENT
    pub static ref CLIENT_IS_RUNNING: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));
    pub static ref CLIENT_IS_SYNC: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));
    pub static ref CLIENT_NODE_KEY: Arc<Mutex<String>> = Arc::new(Mutex::new("".to_string()));
    pub static ref CLIENT_NODE_NAME: Arc<Mutex<String>> = Arc::new(Mutex::new("".to_string()));
    pub static ref CLIENT_LOG_LEVEL: Arc<Mutex<String>> = Arc::new(Mutex::new("".to_string()));
    pub static ref CLIENT_IS_READY: Arc<AtomicBool> = Arc::new(AtomicBool::new(false)); // TODO >>> Finish the impl of this
    pub static ref CLIENT_NODE_CONFIGS: Arc<Mutex<Node>> = Arc::new(Mutex::new(Node::empty_node()));
    pub static ref CLIENT_STATE_MANAGER: Arc<Mutex<ClientState>> = Arc::new(Mutex::new(ClientState::empty()));
    pub static ref CLIENT_CALLBACK_PATTERNS: MyCallbacks = MyCallbacks::new();
    pub static ref CLIENT_IS_CONNECTED: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));
    pub static ref MEDIAN_CON_RESP_TIME: Arc<Mutex<Vec<f64>>> = Arc::new(Mutex::new(vec![0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]));
    pub static ref HOST_ALLOWED_COMMANDS: Arc<Mutex<NetworkMap>> = Arc::new(Mutex::new(NetworkMap::new(Vec::new())));

    // HOST:
    pub static ref HOST_IS_RUNNING: Arc<AtomicBool> = Arc::new(AtomicBool::new(true));
    pub static ref HOST_NODE_NAME: Arc<Mutex<String>> = Arc::new(Mutex::new("".to_string()));
    pub static ref HOST_LOG_LEVEL: Arc<Mutex<String>> = Arc::new(Mutex::new("".to_string()));
    pub static ref HOST_IS_READY: Arc<AtomicBool> = Arc::new(AtomicBool::new(false)); // TODO >>> Finish the impl of this
    pub static ref HOST_COMMAND_PATTERNS: Arc<Mutex<NetworkMap>> = Arc::new(Mutex::new(NetworkMap::new(Vec::new())));
    pub static ref HOST_CALLBACK_PATTERNS: MyCallbacks = MyCallbacks::new();
}

use crate::socket_client::client_logger::log_handler::Logger;
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

// fn main() {}

// -------------------------------------------------------------------------------------------------------------------------------------------------------------
// -> CLIENT:

// use crate::socket_client::client_logger::log_handler::{initialize_client_logs_database_dir, set_client_log_level};
use crate::socket_client::states_manager::manager::inialize_client_status_table_table;
use std::collections::HashMap;

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
pub fn stop_socket_client() {
    CLIENT_IS_RUNNING.store(false, Ordering::SeqCst);
}

pub fn initialize_client_buffer_tables(path: &String) {
    inialize_client_status_table_table(path.clone());
    initialize_client_buffer(path.clone());

    return;
}

// #[derive(Debug, Clone)]
// pub enum ResultType {
//     Empty,
//     Map(HashMap<String, String>),
//     Error(String),
// }

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
    let logger = acquire_logger!("[CLIENT][IS_CLIENT_READY]");

    let client_status = match ClientState::load_from_storage() {
        Ok(c) => c,
        Err(e) => {
            logger.exception(format!("Exception trying to load client status: {:?}", e));
            return false;
        },
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

// pub enum ClientError {
//     ClientIsNotRunning,
//     ClientNotFullyInitialized,
//     NotAbleToReadClientStates,
// }

pub fn client_send_hashmap(command: HashMap<String, String>, priority: u8) -> Result<String, ClientError> {
    if !is_client_ready() {
        println!("Error, client isn't running, pls run the client before try to send something!");
        return Err(ClientError::ClientIsNotRunning);
    }

    // TODO >>> Enhace This Error Handlings, Maybe Add a Logger Here

    let command_instructions = match CommandInstructions::from_string_hashmap(command) {
        Ok(c) => c,
        Err(e) => match e {
            CommandError::InvalidCommand(e) => return Err(ClientError::InvalidCommand(e)),
        },
    };

    let parity_id = match schedule(command_instructions, priority) {
        Ok(parity_id) => parity_id,
        Err(e) => match e {
            scheduler::SchedulingError::CantReadStates => {
                return Err(ClientError::NotAbleToReadClientStates);
            },
            scheduler::SchedulingError::ClientIsntFullyInitialized => {
                return Err(ClientError::ClientNotFullyInitialized);
            },
            scheduler::SchedulingError::CantScheduleCommandsToItself => return Err(ClientError::ClientNotFullyInitialized),
            scheduler::SchedulingError::HandlerDoesntExist => return Err(ClientError::HandlerDoesntExist),
            scheduler::SchedulingError::HostCantSendResponseToItself => return Err(ClientError::HostCantSendResponseToItself),
            scheduler::SchedulingError::ResponseHandlerDoesntExist => return Err(ClientError::ResponseHandlerDoesntExist),
            scheduler::SchedulingError::TargetCantSendResponseToItself => return Err(ClientError::TargetCantSendResponseToItself),
            scheduler::SchedulingError::TargetDoesntExists => return Err(ClientError::TargetDoesntExists),
        },
    };

    Ok(parity_id)
}

pub fn client_send(command: CommandInstructions, priority: u8) -> Result<String, ClientError> {
    if !is_client_ready() {
        println!("Error, client isn't running, pls run the client before try to send something!");
        return Err(ClientError::ClientIsNotRunning);
    }

    let parity_id = match schedule(command, priority) {
        Ok(parity_id) => parity_id,
        Err(e) => match e {
            scheduler::SchedulingError::CantReadStates => {
                return Err(ClientError::NotAbleToReadClientStates);
            },
            scheduler::SchedulingError::ClientIsntFullyInitialized => {
                return Err(ClientError::ClientNotFullyInitialized);
            },
            scheduler::SchedulingError::CantScheduleCommandsToItself => return Err(ClientError::ClientNotFullyInitialized),
            scheduler::SchedulingError::HandlerDoesntExist => return Err(ClientError::HandlerDoesntExist),
            scheduler::SchedulingError::HostCantSendResponseToItself => return Err(ClientError::HostCantSendResponseToItself),
            scheduler::SchedulingError::ResponseHandlerDoesntExist => return Err(ClientError::ResponseHandlerDoesntExist),
            scheduler::SchedulingError::TargetCantSendResponseToItself => return Err(ClientError::TargetCantSendResponseToItself),
            scheduler::SchedulingError::TargetDoesntExists => return Err(ClientError::TargetDoesntExists),
        },
    };

    Ok(parity_id)
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

/// This method can't be toguether with the setup because in the rust based crate
/// the way that callbacks are setted are diferent from the way that they are setted
/// here, they are setted directly because they are a proc macro based system, in cases
/// like python lib for example, this function is needed to set the callbacks using a
/// wrapped mem ref inside a secure closure to make the send and sync work, so this is necessary
/// to make libs that use myscelium in other lenguages because is the simplest way to set remote callbacks
/// so keep that in mind when do some mod to it!
pub fn set_client_callbacks(callbacks: Vec<Callback>) {
    let mut client_handlers: Vec<NodeHandler> = Vec::new();

    for callback in callbacks {
        set_socket_client_transposer_callbacks(callback.actf_name.clone(), callback.callable);

        let handler: NodeHandler = NodeHandler::new(
            callback.actf_name.to_string(),
            callback.parameters.clone(),
            CommandType::ExternalFunction,
            HandlerStatus::NotTested,
            callback.response_structure,
            callback.description,
        );

        client_handlers.push(handler);
    }

    // -> Update client handlers:
    {
        //> Update Client Handlers Globals
        {
            println!("[CLIENT][GLOBAL][Try Lock] - CLIENT_NODE_CONFIGS");
            let mut command_patterns = CLIENT_NODE_CONFIGS.lock();
            println!("[CLIENT][GLOBAL][Lock] - CLIENT_NODE_CONFIGS");
            command_patterns.update_handlers(client_handlers.clone());
            println!("[CLIENT][GLOBAL][Release] - CLIENT_NODE_CONFIGS");
        }

        //> Save client handlers
        let mut client_state = CLIENT_STATE_MANAGER.lock();
        let mut new_client_state = ClientState::load_from_storage().unwrap();
        match new_client_state.update_client_handlers(client_handlers.clone()) {
            Ok(_) => {},
            Err(e) => panic!("Error saving handlers in state manager, error was: {:?}", e),
        };
        new_client_state.update_storage_with_self().unwrap();
        *client_state = new_client_state.clone();
    }
}

pub fn get_socket_client_available_handlers() -> HashMap<String, IndexMap<std::string::String, std::string::String>> {
    get_available_handlers_registered()
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
    thread::sleep(Duration::from_secs(1));
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
pub fn initialize_socket_client(ip: String, port: i32) {
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
    // {
    //     let mut name = CLIENT_NODE_NAME.lock();
    //     *name = client_name.clone();
    //     let mut client_states = ClientState::load_from_storage().unwrap();
    //     client_states.name = Some(name.clone());
    //     client_states.update_schedule_with_this().unwrap();
    // }

    CLIENT_IS_RUNNING.store(true, Ordering::SeqCst);

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
        CLIENT_IS_CONNECTED.store(false, Ordering::SeqCst);
        CLIENT_IS_RUNNING.store(false, Ordering::SeqCst);
    });

    if CLIENT_IS_RUNNING.load(Ordering::SeqCst) {
        loop {
            println!("➡️ Client status: {}", CLIENT_IS_RUNNING.load(Ordering::SeqCst));

            if !CLIENT_IS_RUNNING.load(Ordering::SeqCst) {
                println!("Stop the core!");
                break;
            }

            initialize_socket_client_transposer();
            println!("Socket transposer working!!");
        }
    }

    println!("Socket transposer exited successfully!");
}

pub fn change_client_to_initialized() {
    let mut client_state = CLIENT_STATE_MANAGER.lock();
    client_state.change_initialization_state(true);
    client_state.save_in_storage().unwrap();
}

pub fn setup_socket_client(client_name: String, client_uid: String, buffer_path: String, log_level: String, is_main_process: bool) {
    common::logs_register::register::initialize_logs_file(buffer_path.as_str().clone()).unwrap();
    initialize_client_buffer_tables(&buffer_path);
    set_socket_client_log_level(&log_level);
    set_client_key(client_uid.clone());
    {
        let mut key = CLIENT_NODE_KEY.lock();
        *key = client_uid.clone();
    }
    {
        let mut name = CLIENT_NODE_NAME.lock();
        *name = client_name.clone();
    }

    // -> PRE INITIALIZE CLIENT STATUS AND NODE NETWORK

    if is_main_process {
        // This process diferentiation is required to not overide the initialization when initialize another instance of the client main class in another thred
        // so by doing that the client states continues fixed and with the correct initialization
        let client_version: NodeVersion = CLIENT_VERSION.clone();
        let client_node = Node::new(client_name.clone(), client_uid.clone(), "".to_string(), client_version, Vec::new(), NodeStatus::NotSyncYet);

        {
            println!("[CLIENT][GLOBAL][Try Lock] - CLIENT_NODE_CONFIGS");
            let mut command_patterns = CLIENT_NODE_CONFIGS.lock();
            println!("[CLIENT][GLOBAL][Lock] - CLIENT_NODE_CONFIGS");
            *command_patterns = client_node.clone();
            println!("[CLIENT][GLOBAL][Release] - CLIENT_NODE_CONFIGS");
        }
        {
            let mut client_state = CLIENT_STATE_MANAGER.lock();
            client_state.clean_storage(); // remove any old state
            let new_client_state = ClientState::new(client_name.clone(), client_uid.clone(), NetworkMap::new(Vec::new()), client_node.clone(), false, false, false, false);
            new_client_state.save_in_storage().unwrap();
            *client_state = new_client_state.clone();
        }
    } else {
        {
            let mut client_state = CLIENT_STATE_MANAGER.lock();
            let new_client_state = ClientState::load_from_storage().unwrap();
            *client_state = new_client_state.clone();
        }
    }
}

// -------------------------------------------------------------------------------------------------------------------------------------------------------
// -> HOST

// use crate::common::enhanced_buffer::utilities::CommandType;
// use crate::common::functions::callbacks::extract_arg_types;

use crate::common::client_manager::manager::{clients_manager_initialize_table, set_host_clients_manager__pool_workers_num};
use crate::common::enhanced_buffer::history::register::register::initialize_buffer_history;
use crate::socket_host::host_logger::log_handler::set_host_log_level;
use crate::socket_host::socket_host::get_available_commands_registered;
use crate::socket_host::socket_host::initialize_host;
use crate::socket_host::socket_host::{initialize_host_buffer, set_max_conns};
use crate::socket_host::transposer::set_socket_host_transposer_callbacks;
use crate::socket_host::transposer::{initialize_socket_host_transposer, set_socket_host_transposer_workers_num};

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
    initialize_host_buffer(path.clone());
    initialize_buffer_history(&path.clone()).unwrap();
    common::logs_register::register::initialize_logs_file(path.as_str().clone()).unwrap();
    clients_manager_initialize_table(path.clone());

    return;
}

fn set_socket_host_log_level(log_level: String) {
    set_host_log_level(log_level);
    return;
}

/// This method can't be toguether with the setup because in the rust based crate
/// the way that callbacks are setted are diferent from the way that they are setted
/// here, they are setted directly because they are a proc macro based system, in cases
/// like python lib for example, this function is needed to set the callbacks using a
/// wrapped mem ref inside a secure closure to make the send and sync work, so this is necessary
/// to make libs that use myscelium in other lenguages because is the simplest way to set remote callbacks
/// so keep that in mind when do some mod to it!
pub fn set_host_callbacks(callbacks: HashMap<String, Box<CallbackClosure>>) {
    for (key, callback) in callbacks {
        set_socket_host_transposer_callbacks(key, callback)
    }
}

// pub fn registry_socket_host_client_heartbeat_contact_callback(commands: &PyList) -> PyResult<()> {
//     let mut callback_pattern = HashMap::new();

//     process_commands!(py, commands, callback_pattern);

//     set_heartbeat_callback(callback_pattern);

//     Ok(())
// }

pub fn get_socket_host_available_commands() -> HashMap<String, IndexMap<String, String>> {
    get_available_commands_registered()
}

// > --------------------------------------------------------------------------------------------------------
// > Client Management

// use crate::handle_client_error;
use crate::common::client_manager::manager::get_all_clients;

pub fn load_allowed_clients() {
    let new_allowed_clients_list: Vec<Client> = match get_all_clients() {
        Ok(a) => a,
        Err(e) => match e {
            ClientError::NotAbleToReadClientStates => {
                panic!("Hosts needs at least one client registred to be useful!")
            },
            ClientError::UnexpectedError(e) => {
                panic!("Unexpected error trying to load clients! The error was: {:?}", e)
            },
            _ => {
                panic!("Unexpected error trying to load clients! Can't show error message")
            },
        },
    };

    for client_allowed in new_allowed_clients_list.iter() {
        if !check_if_client_key_exists(client_allowed.client_key.clone()) {
            client_allowed.save_into_db()
        }

        {
            let mut controller = CLIENTS_SYNC_CONTROLLER.lock();
            let _ = controller.add_new_client(client_allowed.client_key.clone().to_string(), 10);
            println!("\nSet clients sync controler to:\n{:?}\n", controller);
        }

        {
            let mut network_map = HOST_COMMAND_PATTERNS.lock();
            let new_node = Node::partially_initialize(client_allowed.client_name.clone(), client_allowed.client_key.clone(), NodeStatus::NotImplemented, None, None, None);
            network_map.add_or_update_if_exists(new_node)
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

// fn remove_all_allowed_clients() {
//     let _ = Client::delete_all();
// }

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

    // -> Partially initialize the host node without the handlers
    let mut global_command_patterns = HOST_COMMAND_PATTERNS.lock();
    let node_version = HOST_VERSION.clone();
    let host_node: Node = Node::new("host".to_string(), "host".to_string(), "".to_string(), node_version, Vec::new(), NodeStatus::Online);
    global_command_patterns.add_or_update_if_exists(host_node);
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
    let _ = Mutex::new(0);
    let _ = Mutex::new(0);

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

    load_allowed_clients();

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
