use crate::common::enhanced_buffer::utilities::CommandType;
use crate::common::functions::python_functions::extract_arg_types;
use crate::common::functions::python_functions::translate_value_to_py;
use crate::common::structs::available_commands::{
    HandlerStatus, Node, NodeHandler, NodeStatus, NodeVersion, VersionIndentifier,
};
use crate::socket_host::client_manager::manager::{
    check_if_client_key_exists, clients_manager_initialize_table,
    set_host_clients_manager__pool_workers_num,
};
use crate::socket_host::client_manager::manager::{Client, ClientError};
use crate::socket_host::host_logger::log_handler::{
    initialize_host_logs_database_dir, set_host_log_level,
};
use crate::socket_host::socket_host::{get_available_commands_registered, initialize_host};
use crate::socket_host::socket_host::{
    initialize_host_buffer, set_heartbeat_callback, set_max_conns,
};
use crate::socket_host::sync_controller::controller::{ClientStatusPoolError, Clients};
use crate::socket_host::transposer::{
    initialize_socket_host_transposer, set_socket_host_transposer_callbacks,
    set_socket_host_transposer_workers_num,
};
use crate::{HOST_COMMAND_PATTERNS, HOST_IS_RUNNING};
use parking_lot::Mutex;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::thread;
use std::time::Duration;

use lazy_static::lazy_static;
use std::sync::Arc;

lazy_static! {
    pub static ref CLIENTS_SYNC_CONTROLLER: Arc<Mutex<Clients>> =
        Arc::new(Mutex::new(Clients::new()));
}

pub fn set_socket_host_transposer_num_of_workers(n_workers: u32) {
    set_socket_host_transposer_workers_num(n_workers);
    return;
}

pub fn set_socket_host_max_connections(n_max_conns: u32) {
    set_host_clients_manager__pool_workers_num(n_max_conns.clone());
    set_max_conns(n_max_conns);
    return;
}

pub fn initialize_host_buffer_tables(path: String) {
    initialize_host_logs_database_dir(path.clone());
    initialize_host_buffer(path.clone());
    clients_manager_initialize_table(path.clone());

    return;
}

pub fn set_socket_host_log_level(log_level: String) {
    set_host_log_level(log_level);
    return;
}

// #[pyfunction]
// fn registry_host_logs_handler(py: Python, commands: &PyList) -> PyResult<()> {
//     let mut callback_pattern = HashMap::new();

//     process_commands!(py, commands, callback_pattern);

//     set_host_logs_handler_callback(callback_pattern);

//     println!("set the log callback");

//     Ok(())
// }

/// Registers a callback function for the socket host to trigger when a client sends a heartbeat message.
///
/// This function updates the global callback that will be called each time the socket host receives a heartbeat
/// message from a client.
///
/// # Parameters
///
/// - `py`: The Python interpreter.
/// - `commands`: A Python list of dictionaries containing the callback function and its expected arguments.
///
/// # Returns
///
/// Returns an empty result if successful, or a Python error if there's a problem with the provided list.
///
/// # Python Binding
///
/// This function is exposed to Python and can be called from a Python script.

pub fn registry_socket_host_client_heartbeat_contact_callback(commands: &PyList) -> PyResult<()> {
    let mut callback_pattern = HashMap::new();

    process_commands!(py, commands, callback_pattern);

    set_heartbeat_callback(callback_pattern);

    Ok(())
}

/// Stops the socket host.
///
/// This function sets the global `HOST_IS_RUNNING` flag to false, indicating that the socket host should stop running.
fn stop_socket_host() {
    HOST_IS_RUNNING.store(false, Ordering::SeqCst);
}

// TODO >>> DEVELOP A MECHANISM TO BE ABLE TO SET RUST FUNCTIONS AS CALLBACKS, THIS ALSO NEEDS TO BE PROCEDURALLY CREATABLE
pub fn registry_socket_host_callbacks(py: Python, commands: &PyList) -> PyResult<()> {
    let mut host_node_handlers: Vec<NodeHandler> = Vec::new();

    let mut callbacks_patterns = HashMap::new();

    for command in commands.iter() {
        let command_dict: &PyDict = command.downcast().unwrap();
        let function: &PyAny = command_dict.get_item("function").unwrap();

        let args_item: &PyAny = command_dict.get_item("args").unwrap();

        // Check if args_item is a dict or a string with the value "None"
        let args_dict: Option<&PyDict>;

        if let Ok(args_as_dict) = args_item.downcast::<PyDict>() {
            args_dict = Some(args_as_dict);
        } else if let Ok(args_as_str) = args_item.extract::<String>() {
            if args_as_str == "None" {
                args_dict = None;
            } else {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "args must be a dict or the string 'None'",
                ));
            }
        } else {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "args must be a dict or the string 'None'",
            ));
        }

        // Extract the Python function name
        let function_name: &str = function.getattr("__name__")?.extract()?;

        // Extract the argument types
        let args_types_value;
        if let Some(args_dict) = args_dict {
            args_types_value = extract_arg_types(args_dict)?;
        } else {
            args_types_value = Value::Array(Vec::new()); // or whatever default value you want to use
        }

        // Store the function name and argument types in the command patterns
        let host_handler: NodeHandler = NodeHandler::new(
            function_name.to_string(),
            args_types_value.clone(),
            CommandType::ExternalFunction,
            HandlerStatus::NotTested,
            HashMap::new(),
            "".to_string(),
        );

        host_node_handlers.push(host_handler);

        let function = function.downcast::<PyFunction>()?.clone();

        let function: Py<PyFunction> = function.into_py(py); // convert &PyAny to Py<PyFunction>
        callbacks_patterns.insert(function_name.to_string(), (function, args_types_value));
    }

    // Now you can use the command_patterns
    set_socket_host_transposer_callbacks(callbacks_patterns);

    let mut global_command_patterns = HOST_COMMAND_PATTERNS.lock();
    let node_version = NodeVersion::cast_version(1, 3, 0, VersionIndentifier::ReleaseCandidate);
    let host_node: Node = Node::new(
        "host".to_string(),
        "host".to_string(),
        "".to_string(),
        node_version,
        host_node_handlers,
        NodeStatus::Online,
    );
    global_command_patterns.add_or_update_if_exists(host_node);

    Ok(())
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
        initialize_socket_host_transposer(py);

        if !HOST_IS_RUNNING.load(Ordering::SeqCst) {
            println!("Stop the core!");
            thread::sleep(Duration::from_secs(7));
            break;
        }
    }

    println!("Socket transposer exited successfully!");
}

pub fn get_socket_host_available_commands() -> HashMap<String, Value> {
    get_available_commands_registered()
}

// > --------------------------------------------------------------------------------------------------------
// > Client Management

use crate::handle_client_error;

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

        println!(
            "Successfully created client: {} of key: {}",
            client_allowed.client_name, client_allowed.client_key
        )
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

// fn set_socket_host_allowed_clients(allowed_clients_list: &PyList) -> PyResult<()> {
//     for client_allowed in allowed_clients_list.iter() {
//         let allowed_clients_dict: &PyDict = client_allowed.downcast().unwrap();

//         let client_type: &PyAny = allowed_clients_dict.get_item("client_type").unwrap();
//         let client_id: &PyAny = allowed_clients_dict.get_item("client_id").unwrap();

//         if let Ok(extracted_client_type) = client_type.extract::<String>() {
//             if let Ok(extracted_client_id) = client_id.extract::<String>() {
//                 register_client(extracted_client_id, extracted_client_type);
//             } else {
//                 return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>("Error: client_id must be a String with 16 characters!"));
//             }
//         } else {
//             return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>("Error: client_type must be a String!"));
//         }
//     }

//     Ok(())
// }
