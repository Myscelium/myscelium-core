// use socket_client;

use crate::common::enhanced_buffer::utilities::{CommandInstructions, CommandType};
use crate::common::functions::advanced_lockers::smart_lock;
use crate::common::structs::available_commands::{
    HandlerStatus, NetworkMap, Node, NodeHandler, NodeStatus, NodeVersion, VersionIndentifier,
};
use crate::socket_client::client_logger::log_handler::{
    initialize_client_logs_database_dir, set_client_log_level,
};
use crate::socket_client::states_manager::manager::{
    inialize_client_status_table_table, ClientState, StateManagerError,
};
use crate::{
    CLIENT_IS_RUNNING, CLIENT_NODE_CONFIGS, CLIENT_NODE_KEY, CLIENT_NODE_NAME, CLIENT_STATE_MANAGER,
};
use parking_lot::Mutex;
use serde_json::Value;
use std::collections::HashMap;
use std::process::Command;
use std::sync::atomic::Ordering;
use std::thread;
use std::time::Duration;

// -> Socket Client main-points:

use crate::common::functions::python_functions::extract_arg_types;
use crate::socket_client::scheduler::{self, schedule};
use crate::socket_client::socket_client;
use crate::socket_client::socket_client::get_available_handlers_registered;
use crate::socket_client::socket_client::{initialize_client, initialize_client_buffer};
use crate::socket_client::transposer::{
    initialize_socket_client_transposer, set_socket_client_transposer_callbacks,
    set_socket_client_transposer_workers_num,
};

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
        }
    };

    if let Some(net_map) = client_status.network_map {
        let mut net_map = net_map;
        {
            match net_map.target_is_reachable(&node_key) {
                Ok(reachable) => {
                    if !reachable {
                        return false;
                    }
                }
                Err(_) => {
                    return false;
                }
            };
        }
        {
            match net_map.target_is_ready(&node_key) {
                Ok(redy) => {
                    if !redy {
                        return false;
                    }
                }
                Err(_) => {
                    return false;
                }
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

pub fn client_send(command: CommandInstructions, priority: u8) -> Result<(), ClientError> {
    if !CLIENT_IS_RUNNING.load(Ordering::SeqCst) {
        println!("Error, client isn't running, pls run the client before try to send something!");
        return Err(ClientError::ClientIsNotRunning);
    }

    let outcome = match schedule(command, priority) {
        Ok(o) => o,
        Err(e) => match e {
            scheduler::SchedulingError::CantReadStates => {
                return Err(ClientError::NotAbleToReadClientStates);
            }
            scheduler::SchedulingError::ClientIsntFullyInitialized => {
                return Err(ClientError::ClientNotFullyInitialized);
            }
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

// // TODO >>> MAKE A RUST BASED METHOD TO SET CALLBACKS IN A WAY THAT IT CAN BE DONE AUTOMATICALLY
// pub fn registry_socket_client_callbacks(py: Python, commands: &PyList) -> PyResult<()> {
//     // For this given data
//     //
//     // special_functions = [{
//     //     "function": get_registered_commands,
//     //     "response_type":"same_as_origin",
//     //     "args": "None",
//     // }, ]
//     //

//     let mut client_handlers: Vec<NodeHandler> = Vec::new();

//     let mut callbacks_patterns = HashMap::new();

//     for command in commands.iter() {
//         let command_dict: &PyDict = command.downcast().unwrap();
//         let function: &PyAny = command_dict.get_item("function").unwrap();

//         let args_item: &PyAny = command_dict.get_item("args").unwrap();

//         // Check if args_item is a dict or a string with the value "None"
//         let args_dict: Option<&PyDict>;

//         if let Ok(args_as_dict) = args_item.downcast::<PyDict>() {
//             args_dict = Some(args_as_dict);
//         } else if let Ok(args_as_str) = args_item.extract::<String>() {
//             if args_as_str == "None" {
//                 args_dict = None;
//             } else {
//                 return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
//                     "args must be a dict or the string 'None'",
//                 ));
//             }
//         } else {
//             return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
//                 "args must be a dict or the string 'None'",
//             ));
//         }

//         // Extract the Python function name
//         let function_name: &str = function.getattr("__name__")?.extract()?;

//         // Extract the argument types
//         let args_types_value;
//         if let Some(args_dict) = args_dict {
//             args_types_value = extract_arg_types(args_dict)?;
//         } else {
//             args_types_value = Value::Array(Vec::new()); // or whatever default value you want to use
//         }

//         let handler: NodeHandler = NodeHandler::new(
//             function_name.to_string(),
//             args_types_value.clone(),
//             CommandType::ExternalFunction,
//             HandlerStatus::NotTested,
//             HashMap::new(),
//             "".to_string(),
//         );
//         client_handlers.push(handler);

//         let function = function.downcast::<PyFunction>()?.clone();

//         let function: Py<PyFunction> = function.into_py(py); // convert &PyAny to Py<PyFunction>
//         callbacks_patterns.insert(function_name.to_string(), (function, args_types_value));
//     }

//     let client_name: String;

//     {
//         let name = CLIENT_NODE_NAME.lock();
//         client_name = name.clone();
//     }

//     let client_key: String;

//     {
//         let mut key = CLIENT_NODE_KEY.lock();
//         client_key = key.clone();
//     }

//     {
//         println!("[CLIENT][GLOBAL][Try Lock] - CLIENT_NODE_CONFIGS");
//         let mut command_patterns = CLIENT_NODE_CONFIGS.lock();
//         println!("[CLIENT][GLOBAL][Lock] - CLIENT_NODE_CONFIGS");

//         let client_version: NodeVersion =
//             NodeVersion::cast_version(1, 3, 0, VersionIndentifier::ReleaseCandidate);
//         let client_node = Node::new(
//             client_name.clone(),
//             client_key.clone(),
//             "".to_string(),
//             client_version,
//             client_handlers,
//             NodeStatus::NotSyncYet,
//         );
//         *command_patterns = client_node.clone();

//         {
//             let mut client_state = CLIENT_STATE_MANAGER.lock();
//             client_state.clean_storage(); // remove any old state
//             let new_client_state = ClientState::new(
//                 client_name.clone(),
//                 client_key.clone(),
//                 NetworkMap::new(Vec::new()),
//                 client_node.clone(),
//                 true,
//                 false,
//                 false,
//                 false,
//             );
//             new_client_state.save_in_storage();
//             *client_state = new_client_state.clone();
//         }

//         println!("[CLIENT][GLOBAL][Release] - CLIENT_NODE_CONFIGS");
//     }

//     // TODO >>> Add the new mechanism of Network Commands here

//     set_socket_client_transposer_callbacks(callbacks_patterns);

//     Ok(())
// }

pub fn get_client_state() -> bool {
    if CLIENT_IS_RUNNING.load(Ordering::SeqCst) {
        true
    } else {
        false
    }
}

pub fn set_client_key(client_key: String) {
    socket_client::set_client_uid(client_key.clone());
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
        println!(
            "➡️ Client status: {}",
            CLIENT_IS_RUNNING.load(Ordering::SeqCst)
        );

        if !CLIENT_IS_RUNNING.load(Ordering::SeqCst) {
            println!("Stop the core!");
            break;
        }

        initialize_socket_client_transposer();
    }

    println!("Socket transposer exited successfully!");
}
