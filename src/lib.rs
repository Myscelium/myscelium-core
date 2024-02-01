#[allow(unused_imports)]
#[allow(unused_extern_crates)]
mod common;

mod socket_client;
mod socket_host;

<<<<<<< HEAD
mod host;
use common::structs::available_commands::Node;
use host::*;

mod client;
=======
mod host_entry_point;
use common::structs::available_commands::Node;
use host_entry_point::*;

mod client_entry_point;
use client_entry_point::*;

use pyo3::prelude::*;
use pyo3::wrap_pyfunction;
>>>>>>> 32e9d99f9affdc9df7245b97686e9aa36c3438de

use lazy_static::lazy_static;

use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use parking_lot::Mutex;

<<<<<<< HEAD
extern crate chrono;
=======
use common::client_network_controller::availability_controller::AllowedNetWorkController;

extern crate chrono;
use crate::chrono::TimeZone;
>>>>>>> 32e9d99f9affdc9df7245b97686e9aa36c3438de
use crate::common::structs::available_commands::NetworkMap;
use crate::socket_client::states_manager::manager::ClientState;

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

    // HOST:
    pub static ref HOST_IS_RUNNING: Arc<AtomicBool> = Arc::new(AtomicBool::new(true));
    pub static ref HOST_NODE_NAME: Arc<Mutex<String>> = Arc::new(Mutex::new("".to_string()));
    pub static ref HOST_LOG_LEVEL: Arc<Mutex<String>> = Arc::new(Mutex::new("".to_string()));
    pub static ref HOST_IS_READY: Arc<AtomicBool> = Arc::new(AtomicBool::new(false)); // TODO >>> Finish the impl of this
    pub static ref HOST_COMMAND_PATTERNS: Arc<Mutex<NetworkMap>> = Arc::new(Mutex::new(NetworkMap::new(Vec::new())));
}

<<<<<<< HEAD
fn main() {}
=======
// #[pyfunction]
// fn registry_socket_host_callbacks (py: Python, commands: &PyList) -> PyResult<()> {

//     for command in commands.iter() {
//         let command_dict: &PyDict = command.downcast().unwrap();
//         let function: &PyAny = command_dict.get_item("function").unwrap();
//         let args_dict: &PyDict = command_dict.get_item("args").unwrap().downcast().unwrap();

//         // Extract the Python function name
//         let function_name: &str = function.getattr("__name__")?.extract()?;

//         let mut command_patterns = HashMap::new();
//         command_patterns.insert(function_name.to_string(), Value::String(args_dict.to_string()));

//         set_socket_host_callbacks (command_patterns);

//         // Convert the args dict to a Vec and then to a tuple
//         let args_vec: Vec<&PyAny> = args_dict.values().extract::<Vec<&PyAny>>()?;
//         let args_tuple: &PyTuple = PyTuple::new(py, args_vec);

//         // Call the Python function with the args
//         let _result = function.call1(args_tuple)?;
//     }

//     Ok(())
// }

// TODO >>> Add a protocol id in the host to check if the client is outdated compared to the host
// TODO >> Create a configs file that automatically be created by Host to configure, client key, or host ip, credentials, data dir, etc..

// -> Entries:

/// The `myscelium_engine` Python module, providing socket host and client functionalities.
///
/// This module contains functions related to both the host and client aspects of the `myscelium_engine`.
/// Users can utilize these functions to set up and manage the socket host and client configurations and operations.
///
/// # Host Functions:
///
/// - `initialize_host_buffer_tables`: Initializes the buffer tables for the host.
/// - `registry_socket_host_callbacks`: Registers callback functions for the socket host.
/// - `initialize_socket_host`: Initializes and starts the socket host.
/// - `get_socket_host_available_commands`: Fetches the list of available commands that the socket host can recognize.
/// - `set_socket_host_max_connections`: Sets the maximum number of connections for the socket host.
/// - `set_socket_host_transposer_num_of_workers`: Sets the number of workers for the socket host transposer.
/// - `set_socket_host_allowed_clients`: Configures the list of clients allowed to connect to the socket host.
/// - `registry_socket_host_client_heartbeat_contact_callback`: Registers a callback function for the socket host to trigger when a client sends a heartbeat message.
/// - `set_socket_host_log_level`: Sets the logging level for the socket host.
/// - `registry_new_allowed_clients`: Registers a new list of allowed clients for the socket host.
///
/// # Client Functions:
///
/// - `initialize_client_buffer_tables`: Initializes the buffer tables for the client.
/// - `registry_socket_client_callbacks`: Registers callback functions for the socket client.
/// - `initialize_socket_client`: Initializes and starts the socket client.
/// - `set_socket_client_transposer_num_of_workers`: Sets the number of workers for the socket client transposer.
/// - `client_send`: Allows the client to send a command to the host.
/// - `set_client_uid`: Sets the unique identifier for the client.
/// - `set_socket_client_log_level`: Sets the logging level for the socket client.
///
/// Note: Some functions, like `registry_host_logs_handler` and `registry_client_logs_handler`, have been commented out and are not currently available.
#[pymodule]
fn myscelium_engine(py: Python<'_>, m: &PyModule) -> PyResult<()> {
    // -> Host
    m.add_function(wrap_pyfunction!(initialize_host_buffer_tables, m)?)?;
    m.add_function(wrap_pyfunction!(registry_socket_host_callbacks, m)?)?;
    m.add_function(wrap_pyfunction!(initialize_socket_host, m)?)?;
    m.add_function(wrap_pyfunction!(get_socket_host_available_commands, m)?)?;
    m.add_function(wrap_pyfunction!(set_socket_host_max_connections, m)?)?;
    m.add_function(wrap_pyfunction!(set_socket_host_transposer_num_of_workers, m)?)?;
    m.add_function(wrap_pyfunction!(set_socket_host_allowed_clients, m)?)?;
    m.add_function(wrap_pyfunction!(registry_socket_host_client_heartbeat_contact_callback, m)?)?;
    // m.add_function(wrap_pyfunction!(registry_host_logs_handler, m)?)?;
    m.add_function(wrap_pyfunction!(set_socket_host_log_level, m)?)?;
    m.add_function(wrap_pyfunction!(registry_new_allowed_clients, m)?)?;

    // -> Client
    m.add_function(wrap_pyfunction!(initialize_client_buffer_tables, m)?)?;
    m.add_function(wrap_pyfunction!(registry_socket_client_callbacks, m)?)?;
    m.add_function(wrap_pyfunction!(initialize_socket_client, m)?)?;
    m.add_function(wrap_pyfunction!(get_client_state, m)?)?;

    m.add_function(wrap_pyfunction!(set_socket_client_transposer_num_of_workers, m)?)?;
    m.add_function(wrap_pyfunction!(client_send, m)?)?;
    m.add_function(wrap_pyfunction!(set_socket_client_log_level, m)?)?;
    m.add_function(wrap_pyfunction!(get_socket_client_available_handlers, m)?)?;
    m.add_function(wrap_pyfunction!(set_client_key, m)?)?;
    m.add_function(wrap_pyfunction!(is_client_ready, m)?)?;
    // m.add_function(wrap_pyfunction!(registry_client_logs_handler, m)?)?;
    m.add_function(wrap_pyfunction!(is_target_ready, m)?)?;

    Ok(())
}

// To call by the python side:

/*

import rust_module  # This is your Rust module compiled as a Python extension

def python_function(name, age, birth):
    # Your function logic here
    pass

rust_module.call_python_function({
    "function": python_function,
    "args": {
        "name": "John",
        "age": 30,
        "birth": "1990-01-01"
    }
})

 */
>>>>>>> 32e9d99f9affdc9df7245b97686e9aa36c3438de
