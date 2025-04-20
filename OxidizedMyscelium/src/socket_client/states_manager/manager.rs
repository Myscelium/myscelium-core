use std::{sync::Arc, thread, time::Duration};

use chrono::Utc;
use lazy_static::lazy_static;
use parking_lot::Mutex;
use rusqlite::params;
use serde::{Deserialize, Serialize};

use crate::{
    common::{
        client_network_controller::availability_controller::NetworkControllerError,
        structs::available_commands::{NetworkMap, Node},
    },
    set_new_path_to_buffer_db, with_connection, ClientError, NodeHandler,
};

use crate::common::sql_pool::pool::{SQLiteConnectionPool, UniqueIdGenerator};

use crate::CLIENT_STATE_MANAGER;

lazy_static! {
    static ref STATES_BUFFER_NAME: Arc<Mutex<String>> = Arc::new(Mutex::new("buffer.db".to_string()));
    static ref STATES_BUFFER_PATH: Arc<Mutex<String>> = Arc::new(Mutex::new("buffer.db".to_string()));
    static ref STATES_NUM_WORKERS: Arc<Mutex<u32>> = Arc::new(Mutex::new(5));
    static ref STATES_BUFFER_POOL: Mutex<SQLiteConnectionPool> = Mutex::new(SQLiteConnectionPool::empty());
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientState {
    pub name: Option<String>,
    pub key: Option<String>,
    pub network_map: Option<NetworkMap>,
    pub client_node_configs: Option<Node>,
    pub is_initialized: Option<bool>,
    pub is_ready: Option<bool>,
    pub is_connected: Option<bool>,
    pub is_sync: Option<bool>,
    pub last_change: Option<f64>,
}

pub fn inialize_client_status_table_table(status_db_spath: String) {
    // Create a global Mutex for demonstration
    let mutex1 = Mutex::new(0);
    let mutex2 = Mutex::new(0);

    set_new_path_to_buffer_db!(STATES_BUFFER_POOL, STATES_NUM_WORKERS, status_db_spath, STATES_BUFFER_NAME);

    with_connection!(STATES_BUFFER_POOL, |conn: &rusqlite::Connection| {
        let result = conn.execute(
            "CREATE TABLE IF NOT EXISTS ClientStates (ID INT PRIMARY KEY, Name TEXT, Key TEXT, NetMap TEXT, ClientNodeConfigs TEXT, IsInitialized BOOL, IsReady BOOL, IsConnected BOOL, IsSync BOOL, LastChange NUMBER)",
            params![],
        );

        match result {
            Ok(_) => {
                println!("Successfully initialize ClientStates table!");
            },
            Err(e) => {
                eprintln!("An error occurred while initializing the ClientState table, the error was: {}", e);
            },
        };
    });
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StateManagerError {
    NotFullyInitialized,
    CantgetStateFromDb(String),
}

impl ClientState {
    pub fn new(name: String, key: String, network_map: NetworkMap, client_node_configs: Node, is_initialized: bool, is_ready: bool, is_connected: bool, is_sync: bool) -> Self {
        Self {
            name: Some(name),
            key: Some(key),
            network_map: Some(network_map),
            client_node_configs: Some(client_node_configs),
            is_initialized: Some(is_initialized),
            is_ready: Some(is_ready),
            is_connected: Some(is_connected),
            is_sync: Some(is_sync),
            last_change: None,
        }
    }

    pub fn update_client_handlers(&mut self, new_handlers: Vec<NodeHandler>) -> Result<(), ClientError> {
        if let Some(client_node_configs) = &mut self.client_node_configs {
            client_node_configs.update_handlers(new_handlers);
            Ok(())
        } else {
            return Err(ClientError::ClientNotFullyInitialized);
        }
    }

    pub fn change_initialization_state(&mut self, new_state: bool) {
        self.is_initialized = Some(new_state)
    }

    pub fn clean_storage(&self) {
        // TODO >>> Finish this method;

        with_connection!(STATES_BUFFER_POOL, |conn: &rusqlite::Connection| {
            let result = conn.execute("DELETE FROM ClientStates;", params![]);

            match result {
                Ok(_) => {
                    println!("Successfully clean ClientStates table");
                },
                Err(e) => {
                    eprintln!("An error occurred while cleaning the ClientStates table: {}", e);
                },
            };
        });
    }

    pub fn is_fully_initialized(&self) -> bool {
        self.name.is_some()
            && self.key.is_some()
            && self.network_map.is_some()
            && self.client_node_configs.is_some()
            && self.is_initialized.is_some()
            && self.is_ready.is_some()
            && self.is_connected.is_some()
            && self.is_sync.is_some()
            && self.last_change.is_some()
    }

    pub fn is_not_fully_initialized(&self) -> bool {
        !self.is_fully_initialized()
    }

    pub fn empty() -> Self {
        Self {
            name: None,
            key: None,
            network_map: None,
            client_node_configs: None,
            is_initialized: None,
            is_ready: None,
            is_connected: None,
            is_sync: None,
            last_change: None,
        }
    }

    pub fn update_storage_with_self(&self) -> Result<(), StateManagerError> {
        with_connection!(STATES_BUFFER_POOL, |conn: &rusqlite::Connection| {
            //let registered_ids = get_registred_ids(conn);
            // let mut id_generator = UniqueIdGenerator { registered_ids: registered_ids };
            // This on top isn't necessary since here will only have one client per per db in each
            // client states table.

            let now = Utc::now();
            let timestamp = now.timestamp() as f64 + (now.timestamp_subsec_millis() as f64 / 1000.0);
            let result = conn.execute(
                "UPDATE ClientStates SET Name = ?, Key = ?, NetMap = ?, ClientNodeConfigs = ?, IsInitialized = ?, IsReady = ?, IsConnected = ?, IsSync = ?, LastChange = ? WHERE ID = ?",
                params![
                    self.name.clone().unwrap_or("".to_string()),
                    self.key.clone().unwrap_or("".to_string()),
                    serde_json::to_string(&self.network_map.clone().unwrap()).unwrap_or("".to_string()),
                    serde_json::to_string(&self.client_node_configs.clone().unwrap()).unwrap_or("".to_string()),
                    self.is_initialized.unwrap_or(false),
                    self.is_ready.unwrap_or(false),
                    self.is_connected.unwrap_or(false),
                    self.is_sync.unwrap_or(false),
                    timestamp,
                    0,
                ],
            );

            match result {
                Ok(_) => {
                    println!("Successfully update state in ClientStates table");
                },
                Err(e) => {
                    eprintln!("An error occurred while updating a cient sate in ClientStates table: {}", e);
                },
            };
        });

        Ok(())
    }

    pub fn save_in_storage(&self) -> Result<(), StateManagerError> {
        with_connection!(STATES_BUFFER_POOL, |conn: &rusqlite::Connection| {
            //let registered_ids = get_registred_ids(conn);
            // let mut id_generator = UniqueIdGenerator { registered_ids: registered_ids };
            // This on top isn't necessary since here will only have one client per per db in each
            // client states table.

            let now = Utc::now();
            let timestamp = now.timestamp() as f64 + (now.timestamp_subsec_millis() as f64 / 1000.0);

            let result = conn.execute(
                "INSERT INTO ClientStates (ID, Name, Key, NetMap, ClientNodeConfigs, IsInitialized, IsReady, IsConnected, IsSync, LastChange) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
                params![
                    0,
                    self.name.clone().unwrap_or("".to_string()),
                    self.key.clone().unwrap_or("".to_string()),
                    serde_json::to_string(&self.network_map.clone().unwrap()).unwrap_or("".to_string()),
                    serde_json::to_string(&self.client_node_configs.clone().unwrap()).unwrap_or("".to_string()),
                    self.is_initialized.unwrap_or(false),
                    self.is_ready.unwrap_or(false),
                    self.is_connected.unwrap_or(false),
                    self.is_sync.unwrap_or(false),
                    timestamp
                ],
            );

            match result {
                Ok(_) => {
                    println!("Successfully saved state in ClientStates table");
                },
                Err(e) => {
                    eprintln!("An error occurred while saving a cient sate in ClientStates table: {}", e);
                },
            };
        });

        Ok(())
    }

    pub fn load_from_storage() -> Result<Self, StateManagerError> {
        // TODO >>> Finish the impl of this method

        with_connection!(STATES_BUFFER_POOL, |conn: &rusqlite::Connection| {
            let mut state: ClientState = ClientState::empty();

            {
                let mut smtp = conn.prepare("SELECT * FROM ClientStates WHERE ID = ?").unwrap();
                let mut commands_iter = smtp
                    .query_map(params![0], |row| {
                        let network: String = row.get(3).unwrap();
                        let client_node: String = row.get(4).unwrap();

                        Ok(Self {
                            name: Some(row.get(1).unwrap()),
                            key: Some(row.get(2).unwrap()),
                            network_map: Some(serde_json::from_str(network.as_str()).unwrap()),
                            client_node_configs: Some(serde_json::from_str::<Node>(client_node.as_str()).unwrap()),
                            is_initialized: Some(row.get(5).unwrap()),
                            is_ready: Some(row.get(6).unwrap()),
                            is_connected: Some(row.get(7).unwrap()),
                            is_sync: Some(row.get(8).unwrap()),
                            last_change: Some(row.get(9).unwrap()),
                        })
                    })
                    .unwrap();
                if let Some(s) = commands_iter.next() {
                    state = s.unwrap();
                } else {
                    return Err(StateManagerError::CantgetStateFromDb("".to_string()));
                }
            }

            return Ok(state);
        })
    }

    pub fn update_schedule_with_this(&self) -> Result<(), StateManagerError> {
        with_connection!(STATES_BUFFER_POOL, |conn: &rusqlite::Connection| {
            // TODO >>> Add the correct parameters here

            //if !self.is_fully_initialized() {
            //    return Err(StateManagerError::NotFullyInitialized);
            //};

            let now = Utc::now();
            let timestamp = now.timestamp() as f64 + (now.timestamp_subsec_millis() as f64 / 1000.0);
            let result = conn.execute(
                "UPDATE ClientStates SET Name = ?, Key = ?, NetMap = ?, ClientNodeConfigs = ?, IsInitialized = ?, IsReady = ?, IsConnected = ?, IsSync = ?, LastChange = ? WHERE ID = ?",
                params![
                    self.name.clone().unwrap_or("".to_string()),
                    self.key.clone().unwrap_or("".to_string()),
                    serde_json::to_string(&self.network_map.clone().unwrap_or(NetworkMap::new(Vec::new()))).unwrap_or("".to_string()),
                    serde_json::to_string(&self.client_node_configs.clone().unwrap_or(Node::empty_node())).unwrap_or("".to_string()),
                    self.is_initialized.unwrap_or(false),
                    self.is_ready.unwrap_or(false),
                    self.is_connected.unwrap_or(false),
                    self.is_sync.unwrap_or(false),
                    timestamp,
                    0
                ],
            );
            match result {
                Ok(_) => {
                    println!("Successfully update client state in ClientStates table");
                },
                Err(e) => {
                    eprintln!("An error occurred while update the client state in the ClientStates table: {}", e);
                },
            };
        });
        Ok(())
    }
}
