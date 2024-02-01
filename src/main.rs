#[allow(unused_imports)]
#[allow(unused_extern_crates)]
mod common;

mod socket_client;
mod socket_host;

mod host;
use common::structs::available_commands::Node;
use host::*;

mod client;

use lazy_static::lazy_static;

use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use parking_lot::Mutex;

extern crate chrono;
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

fn main() {}
