#![deny(unused_must_use)]
#![deny(clippy::disallowed_types)]
#![deny(clippy::await_holding_invalid_type)]
#![deny(clippy::await_holding_lock)] // catches .await while holding a std::sync::MutexGuard
#![allow(unused_imports)]

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

// use ::futures::future::BoxFuture;   // unused legacy import
#[allow(unused_imports)]
#[allow(unused_extern_crates)]
#[deny(warnings)]
#[allow(dead_code, unused_variables)]
#[allow(unused_results)]
use common::enhanced_buffer;
use common::structs::reactive_activator::{BoxFuture, CloneableBox, ReactiveActivator};
use common::types::SchedulingError;
use futures::future::LocalBoxFuture;
use futures::FutureExt;
// use ::futures::FutureExt;           // legacy duplicate import commented out
use indexmap::IndexMap;
use once_cell::sync::Lazy;
use oxidized_myscelium_macros::callback;

use lazy_static::lazy_static;
use socket_client::response_watcher::watch_response;
use syn::buffer;
use tokio::runtime::Runtime;
use tokio::signal;
use tokio::sync::Mutex;
use tokio::sync::{Notify, Semaphore};

use core::panic;
use std::pin::Pin;

use std::sync::atomic::AtomicBool;
use std::sync::{mpsc, Arc};

pub use crate::common::structs::callbacks_structure::FunctionMetadata;

extern crate chrono;

pub use crate::common::client_manager::manager::ClientError;
pub use crate::common::enhanced_buffer::utilities::Command;
use crate::common::structs::callbacks::{CallbackClosure, MyCallbacks};
use crate::socket_client::client_logger::log_handler::set_client_log_level;
pub use crate::socket_client::response_watcher::WatcherError;
pub use common::client_network_controller::availability_controller::AllowedNetWorkController;
pub use common::enhanced_buffer::utilities::CommandError;
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
use crate::socket_host::task_manager::manager::NodesTaskManager;
pub use crate::socket_host::transposer_functions::handle_direct_function::ProcessResult;
pub use socket_host::socket_host::set_heartbeat_callback;
use tokio::runtime::Handle;

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
    pub static ref CLIENT_CALLBACK_PATTERNS: MyCallbacks = MyCallbacks::new(); // TODO >>> Verify if this doesn't need to switch to tokio mutex because of the async code that uses it!
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
    pub static ref TASKS_MANAGER: Arc<Mutex<NodesTaskManager>> = Arc::new(Mutex::new(NodesTaskManager::new_empty()));
}

// Shared runtime for non-Send transposer functions
static TRANSPOSER_RUNTIME: Lazy<tokio::sync::Mutex<Runtime>> = Lazy::new(|| {
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("Failed to create transposer runtime");
    tokio::sync::Mutex::new(rt)
});

// -> HOST BUFFER TRANSPOSITION REACTIVE ACTIVATOR:

// (1) Change HOST_BUFFER_ACTIVATION_CONTROLLER to mirror the client's pattern:
static HOST_BUFFER_ACTIVATION_CONTROLLER: Lazy<tokio::sync::Mutex<Option<Arc<ReactiveActivator>>>> = Lazy::new(|| tokio::sync::Mutex::new(None));

///  * `action()`  — spawns `initialize_socket_host_transposer()` on
///                  a blocking thread tied to the current runtime.
///  * `condition()` — **async predicate**; it resolves to `true` once
///                    the host's auto-collect queue is empty.  
///                    A background task polls the queue every 50 ms,
///                    prints debug info, and flips an `AtomicBool`.
pub async fn init_host_reactive_activator() {
    // ───── 1. Lock global controller & bail if already initialised ─────
    let mut guard = HOST_BUFFER_ACTIVATION_CONTROLLER.lock().await;
    if guard.is_some() {
        println!("[Host] ReactiveActivator already initialised.");
        return;
    }
    println!("[Host] Initialising ReactiveActivator …");

    use std::{future::Future, pin::Pin};

    // ───── 2. ACTION: identical pattern to the client's, but host fn ───
    let action: Arc<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync> = Arc::new(move || {
        Box::pin(async move {
            let result = tokio::task::spawn_blocking(|| {
                let rt_guard = TRANSPOSER_RUNTIME.blocking_lock();
                rt_guard.block_on(async {
                    if let Err(e) = initialize_socket_host_transposer().await {
                        eprintln!("[Host] transposer error: {e:?}");
                    }
                })
            })
            .await;

            if let Err(e) = result {
                eprintln!("[Host] spawn_blocking error: {e:?}");
            }
        })
    });

    // ───── 4. CONDITION: async predicate w/ background poll + debug ────
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };

    let condition: Arc<dyn Fn() -> Pin<Box<dyn Future<Output = bool> + Send>> + Send + Sync> = {
        Arc::new(move || {
            Box::pin(async move {
                let mut schedule = enhanced_buffer::buffer_down_manager::buffer_down_list_schedule().await.unwrap_or_default();
                schedule.retain(|cmd| cmd.auto_collect);
                let empty = schedule.is_empty();
                println!("[Host][Condition-poll] auto_collect queue len = {}, empty = {}", schedule.len(), empty);
                empty
            })
        })
    };

    // ───── 5. Initialize a semaphore to control the transposition execution flow
    let transposer_sem = Arc::new(Semaphore::new(1));

    // ───── 6. Build & start the activator while still holding the lock ──
    let activator = ReactiveActivator::new(action, condition, transposer_sem);
    activator.start().await;
    *guard = Some(activator); // store in the global controller

    println!("[Host] ReactiveActivator initialised and running.");
    // lock released when `guard` goes out of scope
}

// -> CLIENT BUFFER TRANSPOSITION REACTIVE ACTIVATOR:

static CLIENT_BUFFER_ACTIVATION_CONTROLLER: Lazy<tokio::sync::Mutex<Option<Arc<ReactiveActivator>>>> = Lazy::new(|| tokio::sync::Mutex::new(None));

// macro_rules! acquire_client_logger {
//     ($section_name:expr) => {{
//         let client_log_level;
//         {
//             let log_level = CLIENT_LOG_LEVEL.lock().await.clone();
//             client_log_level = log_level.clone();
//         }
//         Logger::new(client_log_level, $section_name).await
//     }};
// }

pub async fn init_client_reactive_activator() {
    // 1) Grab a logger (held only briefly)
    let mut guard = CLIENT_BUFFER_ACTIVATION_CONTROLLER.lock().await; // 🔒 LOCKED

    // 2) Avoid double‐init
    if guard.is_some() {
        return;
    }

    // ─────────────────────── ACTION ────────────────────────────────────────────

    use std::{future::Future, pin::Pin};

    // This closure moves `rt_handle.clone()` into a non‐Send blocking thread,
    // then builds a brand‐new current_thread runtime inside that thread and runs
    // `initialize_socket_client_transposer().await` there. This avoids deadlocks
    // if `initialize_socket_client_transposer()` itself is not Send.
    let action: Arc<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync> = Arc::new(move || {
        // We capture nothing non-Send in this block,
        // so the resulting future is Send.
        Box::pin(async move {
            // Use shared runtime to avoid creation overhead
            let result = tokio::task::spawn_blocking(|| {
                let rt_guard = TRANSPOSER_RUNTIME.blocking_lock();
                rt_guard.block_on(async {
                    if let Err(e) = initialize_socket_client_transposer().await {
                        eprintln!("[Client] transposer error: {e:?}");
                    }
                })
            })
            .await;

            if let Err(e) = result {
                eprintln!("[Client] spawn_blocking error: {e:?}");
            }
        })
    });

    // ───────────────────── CONDITION ────────────────────────────────────────────

    let condition: Arc<dyn Fn() -> Pin<Box<dyn Future<Output = bool> + Send>> + Send + Sync> = {
        Arc::new(move || {
            Box::pin(async move {
                let mut schedule = enhanced_buffer::buffer_down_manager::buffer_down_list_schedule().await.unwrap_or_default();
                schedule.retain(|cmd| cmd.auto_collect);
                schedule.is_empty()
            })
        })
    };

    // ───────────────────── ACTIVATOR ─────────────────────────────────────────────

    // ───── Initialize a semaphore to control the transposition execution flow
    let transposer_sem = Arc::new(Semaphore::new(1));

    // Create and start the ReactiveActivator as before—`start().await` will return
    // immediately because `action()` itself (when invoked) only does `spawn_blocking`.
    let activator = ReactiveActivator::new(action, condition, transposer_sem);
    activator.start().await; // ⬅️  awaits **while the mutex is held**

    *guard = Some(activator); // still inside the lock

    println!("Exiting buffer reactive activator initializer")
}

use crate::socket_client::client_logger::log_handler::Logger;
macro_rules! acquire_logger {
    ($section_name:expr) => {{
        let client_log_level;
        {
            let log_level = CLIENT_LOG_LEVEL.lock().await.clone();
            client_log_level = log_level.clone();
        }
        Logger::new(client_log_level, $section_name).await
    }};
}

// fn main() {}

// -------------------------------------------------------------------------------------------------------------------------------------------------------------
// -> CLIENT:

use crate::socket_client::states_manager::manager::inialize_client_status_table_table;
use std::collections::HashMap;

use std::sync::atomic::Ordering;
use std::thread;
use std::time::Duration;

// -> Socket Client main-points:

use crate::socket_client::scheduler::{self, schedule};
use crate::socket_client::socket_client::get_available_handlers_registered;
use crate::socket_client::socket_client::{initialize_client, initialize_client_buffer};
use crate::socket_client::transposer::{initialize_socket_client_transposer, set_socket_client_transposer_callbacks, set_socket_client_transposer_workers_num};

pub async fn set_socket_client_transposer_num_of_workers(n_workers: u32) {
    set_socket_client_transposer_workers_num(n_workers).await;
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

pub async fn initialize_client_buffer_tables(path: &str) {
    inialize_client_status_table_table(path.to_owned()).await;
    initialize_client_buffer(path.to_owned()).await;
}

// #[derive(Debug, Clone)]
// pub enum ResultType {
//     Empty,
//     Map(HashMap<String, String>),
//     Error(String),
// }

pub async fn is_target_ready(node_key: String) -> bool {
    let client_status = match ClientState::load_from_storage().await {
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

    true
}

pub async fn is_client_ready() -> bool {
    let logger = acquire_logger!("[CLIENT][IS_CLIENT_READY]");

    let client_status: ClientState = match ClientState::load_from_storage().await {
        Ok(c) => c,
        Err(e) => {
            logger.exception(format!("Exception trying to load client status: {:?}", e)).await;
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

    true
}

// pub enum ClientError {
//     ClientIsNotRunning,
//     ClientNotFullyInitialized,
//     NotAbleToReadClientStates,
// }

fn translate_scheduling_error<T>(res: Result<T, SchedulingError>) -> Result<T, ClientError> {
    match res {
        Ok(parity_id) => Ok(parity_id),
        Err(e) => Err(e.into()),
    }
}

pub async fn client_send_hashmap(command: HashMap<String, String>, priority: u8) -> Result<String, ClientError> {
    if !is_client_ready().await {
        println!("Error, client isn't running, pls run the client before try to send something!");
        return Err(ClientError::ClientIsNotRunning(command.get("target").unwrap().clone()));
    }

    // > Add the origin to the command: (this points to the self uid)
    let mut command = command;
    let mut client_uid: String = "".to_string();
    {
        let node = CLIENT_NODE_CONFIGS.lock().await;
        if let Some(key) = &node.key {
            client_uid = key.clone()
        }
    }

    command.insert("origin".to_string(), format!("ClientKey({})", client_uid));

    // TODO >>> Enhace This Error Handlings, Maybe Add a Logger Here

    // -> Downcast the command:
    let command_instructions = match CommandInstructions::from_string_hashmap(command) {
        Ok(c) => c,
        Err(e) => match e {
            CommandError::InvalidCommand(e) => return Err(ClientError::InvalidCommand(e)),
            CommandError::DeserializationError(e) => return Err(ClientError::InvalidCommand(e.to_string())),
            CommandError::InvalidResponse(e) => unreachable!("Unexpecte Error: {:?}", e),
            CommandError::NotAJsonObject => unimplemented!("Unexpecte Error: Not a json object!"),
            CommandError::UnexpectedError(e) => panic!("Received an unexpected error: {:?}", e),
        },
    };

    let parity_id = translate_scheduling_error(schedule(command_instructions, priority).await)?;

    Ok(parity_id)
}

pub async fn client_send(command: CommandInstructions, priority: u8) -> Result<String, ClientError> {
    if !is_client_ready().await {
        println!("Error, client isn't running, pls run the client before try to send something!");
        return Err(ClientError::ClientIsNotRunning(command.target.to_string()));
    }

    let parity_id = translate_scheduling_error(schedule(command, priority).await)?;

    Ok(parity_id)
}

/// Allows to wait a response by parity id, some conditions needs to be satisfied foe that:
/// 1. Command needs to have auto collect == false to transposer not auto collect it
/// 2. parity id needs to be the parity id assigned to the command, this is returned to send
/// 3. ensure client is initialized, you can't waith a response if client isn't initialized
pub async fn client_wait_response(parity_id: String, wait_for: u64) -> Result<Command, WatcherError> {
    watch_response(parity_id, chrono::Duration::seconds(wait_for as i64)).await
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
pub async fn set_socket_client_log_level(log_level: &str) {
    set_client_log_level(log_level.to_owned()).await;
}

/// This method can't be toguether with the setup because in the rust based crate
/// the way that callbacks are setted are diferent from the way that they are setted
/// here, they are setted directly because they are a proc macro based system, in cases
/// like python lib for example, this function is needed to set the callbacks using a
/// wrapped mem ref inside a secure closure to make the send and sync work, so this is necessary
/// to make libs that use myscelium in other lenguages because is the simplest way to set remote callbacks
/// so keep that in mind when do some mod to it!
pub async fn set_client_callbacks(callbacks: Vec<Callback>) {
    let mut client_handlers: Vec<NodeHandler> = Vec::new();

    for callback in callbacks {
        set_socket_client_transposer_callbacks(callback.actf_name.clone(), callback.callable).await;

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
            let mut command_patterns = CLIENT_NODE_CONFIGS.lock().await;
            println!("[CLIENT][GLOBAL][Lock] - CLIENT_NODE_CONFIGS");
            command_patterns.update_handlers(client_handlers.clone());
            println!("[CLIENT][GLOBAL][Release] - CLIENT_NODE_CONFIGS");
        }

        //> Save client handlers
        let mut new_client_state = ClientState::load_from_storage().await.unwrap();
        match new_client_state.update_client_handlers(client_handlers.clone()) {
            Ok(_) => {},
            Err(e) => panic!("Error saving handlers in state manager, error was: {:?}", e),
        };
        new_client_state.update_storage_with_self().await.unwrap();
        let mut client_state = CLIENT_STATE_MANAGER.lock().await;
        *client_state = new_client_state.clone();
    }
}

pub async fn get_socket_client_available_handlers() -> HashMap<String, IndexMap<std::string::String, std::string::String>> {
    get_available_handlers_registered().await
}

pub fn get_client_state() -> bool {
    thread::sleep(Duration::from_secs(1));
    CLIENT_IS_RUNNING.load(Ordering::SeqCst)
}

pub async fn set_client_key(client_key: String) {
    socket_client::socket_client::set_client_uid(client_key.clone()).await;
    {
        let mut key = CLIENT_NODE_KEY.lock().await;
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

    let addr = format!("{ip}:{port}");
    let shutdown = Arc::new(Notify::new());
    let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().expect("Failed to create Tokio runtime");

    rt.block_on(async {
        // ── 1. spawn Ctrl-C watcher (wakes every task that awaits `shutdown.notified()`)
        let shutdown_watcher = shutdown.clone();
        tokio::spawn(async move {
            signal::ctrl_c().await.unwrap();
            println!("CTRL-C received – notifying shutdown");
            CLIENT_IS_RUNNING.store(false, Ordering::SeqCst);
            shutdown_watcher.notify_waiters();
        });

        // ── 2. spawn the whole client
        let shutdown_client = shutdown.clone();
        initialize_client(addr, shutdown_client.clone()).await;
        shutdown_client.notify_waiters(); // stop everything when it returns
    });

    // **This** is the key: block the _main_ thread on the runtime until
    // ── 3. HOLD the runtime alive until shutdown is signalled

    let shutdown_handler = shutdown.clone();
    rt.block_on(async {
        shutdown_handler.notified().await; // wait here
    });

    println!("Socket transposer exited successfully!");
}

pub async fn change_client_to_initialized() {
    let mut client_state = CLIENT_STATE_MANAGER.lock().await; // <-- .await here
    client_state.change_initialization_state(true);
    client_state.save_in_storage().await.unwrap(); // guard is async-aware
}

pub async fn setup_socket_client(client_name: String, client_uid: String, buffer_path: String, log_level: String, is_main_process: bool) -> Result<(), String> {
    println!("Setting up the socket clinet in: {:?}", buffer_path);
    common::logs_register::register::initialize_logs_file(buffer_path.as_str()).await.unwrap();
    initialize_client_buffer_tables(&buffer_path).await;
    set_socket_client_log_level(&log_level).await;
    set_client_key(client_uid.clone()).await;

    {
        let mut key = CLIENT_NODE_KEY.lock().await;
        *key = client_uid.clone();
    }
    {
        let mut name = CLIENT_NODE_NAME.lock().await;
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
            let mut command_patterns = CLIENT_NODE_CONFIGS.lock().await;
            println!("[CLIENT][GLOBAL][Lock] - CLIENT_NODE_CONFIGS");
            *command_patterns = client_node.clone();
            println!("[CLIENT][GLOBAL][Release] - CLIENT_NODE_CONFIGS");
        }
        {
            let mut client_state = CLIENT_STATE_MANAGER.lock().await;
            client_state.clean_storage().await; // remove any old state
            let new_client_state = ClientState::new(client_name.clone(), client_uid.clone(), NetworkMap::new(Vec::new()), client_node.clone(), false, false, false, false);
            new_client_state.save_in_storage().await.map_err(|e| format!("Error trying to save client status in storage: {:?}", e))?;
            *client_state = new_client_state.clone();
        }
    } else {
        let mut loading_attempts: u64 = 0u64;
        loop {
            thread::sleep(Duration::from_secs(1));

            let new_client_state = ClientState::load_from_storage().await.map_err(|e| format!("Error trying to load client status in storage, error: {:?}", e))?;

            {
                let mut client_state = CLIENT_STATE_MANAGER.lock().await;
                *client_state = new_client_state.clone();
                if client_state.is_fully_initialized() {
                    break;
                }
            }

            if loading_attempts > 10u64 {
                return Err("Client STATE_MANAGER ins't fully initialized!".to_string());
            }

            println!("Client STATE_MANAGER isn't fully initialized, trying again in 1 sec!");

            loading_attempts += 1;
        }
    }

    Ok(())
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
    pub static ref CLIENTS_SYNC_CONTROLLER: Arc<tokio::sync::Mutex<Clients>> = Arc::new(tokio::sync::Mutex::new(Clients::new()));
}

async fn set_socket_host_transposer_num_of_workers(n_workers: u32) {
    set_socket_host_transposer_workers_num(n_workers).await;
}

async fn set_socket_host_max_connections(n_max_conns: u32) {
    set_host_clients_manager__pool_workers_num(n_max_conns).await;
    set_max_conns(n_max_conns).await;
}

async fn initialize_host_buffer_tables(path: String) -> Result<(), std::io::Error> {
    initialize_host_buffer(path.clone()).await;
    initialize_buffer_history(&path.clone()).await?;
    common::logs_register::register::initialize_logs_file(path.as_str()).await?;
    clients_manager_initialize_table(path.clone()).await;
    Ok(())
}

async fn set_socket_host_log_level(log_level: String) {
    set_host_log_level(log_level).await;
}

/// This method can't be toguether with the setup because in the rust based crate
/// the way that callbacks are setted are diferent from the way that they are setted
/// here, they are setted directly because they are a proc macro based system, in cases
/// like python lib for example, this function is needed to set the callbacks using a
/// wrapped mem ref inside a secure closure to make the send and sync work, so this is necessary
/// to make libs that use myscelium in other lenguages because is the simplest way to set remote callbacks
/// so keep that in mind when do some mod to it!
pub async fn set_host_callbacks(callbacks: HashMap<String, Box<CallbackClosure>>) {
    for (key, callback) in callbacks {
        set_socket_host_transposer_callbacks(key, callback).await
    }
}

pub async fn get_socket_host_available_commands() -> HashMap<String, IndexMap<String, String>> {
    get_available_commands_registered().await
}

// > --------------------------------------------------------------------------------------------------------
// > Client Management

// use crate::handle_client_error;
use crate::common::client_manager::manager::get_all_clients;

pub enum ClientLoaderError {
    ClientDoesNotExist(String),
    ClientAlreadyExist(String),
    UnexpectedError(String),
    NotAbleToReadClientStates,
}

pub async fn load_allowed_clients() -> Result<(), ClientLoaderError> {
    let new_allowed_clients_list: Vec<Client> = get_all_clients().await?;

    println!("All client's retrived!");

    //> PRE POPULATE THE HOST TASK MANAGER WITH HOST NODE
    {
        let mut tasks_manager = TASKS_MANAGER.lock().await;
        tasks_manager.add_node("Host".to_string()).unwrap();
    }

    println!("Host node added!");

    //> Populate the controllers with the nodes of the network
    for client_allowed in new_allowed_clients_list.iter() {
        if !check_if_client_key_exists(client_allowed.client_key.clone()).await? {
            client_allowed.save_into_db().await?;
        }

        // -> POPULATE THE HOST SYNC CONTROLLER NODES

        {
            let mut controller = CLIENTS_SYNC_CONTROLLER.lock().await;
            let _ = controller.add_new_client(client_allowed.client_key.clone().to_string(), 10);
            println!("\nSet clients sync controler to:\n{:?}\n", controller);
        }

        // -> POPULATE THE HOST NETWORK NODES

        {
            let mut network_map = HOST_COMMAND_PATTERNS.lock().await;
            let new_node = Node::partially_initialize(client_allowed.client_name.clone(), client_allowed.client_key.clone(), NodeStatus::NotImplemented, None, None, None);
            network_map.add_or_update_if_exists(new_node)
        }

        // -> POPULATE THE HOST TASK MANAGER NODES

        {
            let mut tasks_manager = TASKS_MANAGER.lock().await;
            tasks_manager.add_node(client_allowed.client_key.clone()).unwrap();
        }

        println!("Successfully created client: {} of key: {}", client_allowed.client_name, client_allowed.client_key)
    }

    println!("All nodes loaded!");

    Ok(())
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

pub async fn setup_socket_host(buffer_path: &str, log_level: &str, n_workers: &u32, n_max_conns: &u32) -> Result<(), std::io::Error> {
    initialize_host_buffer_tables(buffer_path.to_owned()).await?;
    set_socket_host_log_level(log_level.to_owned()).await;
    set_socket_host_transposer_num_of_workers(*n_workers).await;
    set_socket_host_max_connections(*n_max_conns).await;

    // -> Partially initialize the host node without the handlers
    let mut global_command_patterns = HOST_COMMAND_PATTERNS.lock().await;
    let node_version = HOST_VERSION.clone();
    let host_node: Node = Node::new("host".to_string(), "host".to_string(), "".to_string(), node_version, Vec::new(), NodeStatus::Online);
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
    let _ = Mutex::new(0);
    let _ = Mutex::new(0);

    // Spawn a thread to periodically check for deadlocks
    thread::spawn(move || {
        // install your Ctrl+C handler
        ctrlc::set_handler(move || {
            if HOST_IS_RUNNING.load(Ordering::SeqCst) {
                println!("\nreceived Ctrl+C!\n");
                stop_socket_host();
            }
        })
        .expect("Error setting Ctrl-C handler");

        // Build exactly one Tokio runtime here
        let rt: Runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(10).enable_all().build().expect("Failed to create Tokio runtime");

        // use that same `rt` to first load clients, then initialize the host
        rt.block_on(async {
            init_host_reactive_activator().await; // Run this here to restrict the setup just to the socker server process and bind the trasnposition reactor to this process

            // Load allowed clients
            if let Err(e) = load_allowed_clients().await {
                match e {
                    ClientLoaderError::NotAbleToReadClientStates => {
                        panic!("Hosts needs at least one client registered to be useful!")
                    },
                    ClientLoaderError::UnexpectedError(inner) => {
                        panic!("Unexpected error trying to load clients: {:?}", inner)
                    },
                    ClientLoaderError::ClientAlreadyExist(inner) => {
                        panic!("Clients should never be created during loading: {:?}", inner)
                    },
                    ClientLoaderError::ClientDoesNotExist(inner) => {
                        panic!("Lookups during loading should never fail: {:?}", inner)
                    },
                }
            }

            // Now drive your `initialize_host(...)` future
            if let Err(e) = initialize_host(format!("{}:{}", ip, port), client_id.clone()).await {
                panic!("Error from host: {:?}", e);
            }

            println!("Socket host exited successfully!");
        });

        // once the above block returns, host has cleanly shut down!
    });
    loop {
        // initialize_socket_host_transposer();
        if !HOST_IS_RUNNING.load(Ordering::SeqCst) {
            println!("Stop the core!");
            thread::sleep(Duration::from_secs(7));
            break;
        }
        thread::sleep(Duration::from_secs(5));
    }

    // println!("Socket transposer exited successfully!");
}
