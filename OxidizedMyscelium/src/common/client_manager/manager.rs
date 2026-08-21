// SPDX-License-Identifier: MPL-2.0
// Copyright © 2021-2026 Cristian Camargo Filho

use lazy_static::lazy_static;
use serde_json::to_string;

#[macro_use]
use crate::{with_connection, set_new_path_to_buffer_db};
use crate::common::sql_pool::pool::{
    SQLiteConnectionPool, UniqueIdGenerator, UniqueParityIdGenerator,
};

use rusqlite::params;
use serde_json::{from_str, to_string_pretty, Value};

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;

use rusqlite::{Connection, Result};

use std::thread;
use std::time::Duration;

use rusqlite::Row;
use rusqlite::Statement;

use std::time::SystemTime;
use std::time::UNIX_EPOCH;

#[macro_export]
macro_rules! handle_manager_client_error {
    ($client_result:expr) => {
        match $client_result {
            Ok(c) => c, // Return the unwrapped client directly
            Err(e) => {
                match e {
                    ClientError::ClientAlreadyExist(c) => {
                        println!("Error client: {} already exist", c);
                    }
                    ClientError::ClientDoesNotExist(c) => {
                        println!("Error client: {} doesn't exist", c);
                    }
                    ClientError::UnexpectedError(e) => {
                        println!("Get a unexpected error: {}", e);
                    }
                    _ => {
                        println!("Get a unexpected error!");
                    }
                }
                panic!("Client error encountered!"); // Panic after printing the error
            }
        }
    };
}

lazy_static! {
    static ref SQL_NAME: Arc<Mutex<String>> = Arc::new(Mutex::new("Data.db".to_string()));
    static ref SQL_PATH: Arc<Mutex<String>> = Arc::new(Mutex::new("Data.db".to_string()));
    static ref NUM_WORKERS: Arc<Mutex<u32>> = Arc::new(Mutex::new(5));
    static ref SQL_POOL: Mutex<SQLiteConnectionPool> = Mutex::new(SQLiteConnectionPool::empty());
}

pub fn set_host_clients_manager__pool_workers_num(n_workers: u32) {
    let mut default_num_of_workers = NUM_WORKERS.lock();

    *default_num_of_workers = n_workers;
}

pub fn clients_manager_initialize_table(sql_path: String) {
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

    set_new_path_to_buffer_db!(SQL_POOL, NUM_WORKERS, sql_path, SQL_NAME);

    with_connection!(SQL_POOL, |conn: &rusqlite::Connection| {
        let result = conn.execute(
            "CREATE TABLE IF NOT EXISTS Clients (ID INT PRIMARY KEY, ClientName TEXT, ClientKey TEXT, ClientType TEXT, PermissionGroup TEXT, SuperUser BOOL, LastContact NUMBER, MaxSubChannels NUMBER, OwnedSubChannelsKeys TEXT, SubChannelsInUse NUMBER, Handlers TEXT, Syncronized BOOL)",
            params![],
        );

        match result {
            Ok(_) => {
                println!("Successfully initialize Clients table!");
            }
            Err(e) => {
                eprintln!(
                    "An error occurred while scheduling the command in the Clients table: {}",
                    e
                );
            }
        };
    });
}

#[derive(Debug, Clone)]
pub enum ClientError {
    ClientDoesNotExist(String),
    ClientAlreadyExist(String),
    UnexpectedError(String),
    InvalidCommand(String),
    ClientIsNotRunning,
    ClientNotFullyInitialized,
    NotAbleToReadClientStates,
    TargetDoesntExists,
    HandlerDoesntExist,
    ResponseHandlerDoesntExist,
    CantScheduleCommandsToItself,
    HostCantSendResponseToItself,
    TargetCantSendResponseToItself,
}

#[derive(Debug, Clone)]
pub struct Client {
    pub client_id: u32,
    pub client_name: String,
    pub client_key: String,
    client_type: String,
    permission_group: String,
    is_super_user: bool,
    pub last_contact: f64,
    max_sub_channels: u32,
    owned_sub_channels_keys: Vec<String>,
    sub_channels_in_use: u32,
    client_handlers: Vec<HashMap<String, Value>>, // To Store client Handlers
    syncronized: bool,
}

// > Get client by key
// > Get client by name
// > edit client
// > change key
// > new client
// > client from
// > delet client

impl Client {
    /// Constructs a new `Client` instance.
    ///
    /// This method creates a new client with specified attributes. If `client_handlers` is not provided
    /// or is empty, it initializes `client_handlers` with a default set of handler mappings.
    ///
    /// # Arguments
    /// * `client_name` - A `String` representing the unique name of the client.
    /// * `client_key` - A `String` used for client verification.
    /// * `client_type` - A `String` indicating the type of client.
    /// * `permission_group` - A `String` representing the permission group the client belongs to.
    /// * `is_super_user` - A boolean indicating whether the client has superuser privileges.
    /// * `max_sub_channels` - A `u32` representing the maximum number of subchannels a client can own.
    /// * `owned_sub_channels_keys` - A `Vec<String>` containing keys to the subchannels owned by the client.
    /// * `client_handlers` - A `Vec<HashMap<String, Value>>` containing the client's handlers.
    ///   If empty, a predefined set of handlers is loaded as a default.
    ///
    /// # Returns
    /// A `Result` which is:
    /// * `Ok` containing the new `Client` instance.
    /// * `Err` containing a `ClientError` if the client could not be created.
    ///
    /// # Examples
    /// ```
    /// let client = Client::new(
    ///     "client_name".to_string(),
    ///     "client_key".to_string(),
    ///     "client_type".to_string(),
    ///     "permission_group".to_string(),
    ///     true,
    ///     10,
    ///     vec!["key1".to_string(), "key2".to_string()],
    ///     vec![],
    /// ).unwrap();
    /// ```
    ///
    /// # Errors
    /// This method will return an `Err` if the `client_id` cannot be generated or if any other part of
    /// the client creation process fails.
    pub fn new(
        client_name: String,
        client_key: String,
        client_type: String,
        permission_group: String,
        is_super_user: bool,
        max_sub_channels: u32,
        owned_sub_channels_keys: Vec<String>,
        mut client_handlers: Vec<HashMap<String, Value>>,
    ) -> Result<Self, ClientError> {
        let client_id;

        {
            let registered_ids = get_registered_ids();
            let mut id_generator = UniqueIdGenerator {
                registered_ids: registered_ids,
            };
            client_id = id_generator.gen();
        }

        // -> Store the default handlers

        // TODO >>> Add the new client handlers mechanism

        if client_handlers.is_empty() {
            // r# means raw string
            let json_str = r#"[
                {
                    "function": "get_avaliable_handlers",
                    "kwargs": {
                        "arg1": "int",
                        "arg2": "str"
                    }
                },
                {
                    "function": "update_avaliable_commands",
                    "kwargs": {
                        "commands": "dict"
                    }
                }
            ]"#;

            client_handlers =
                serde_json::from_str::<Vec<HashMap<String, Value>>>(json_str).unwrap();
        }

        Ok(Self {
            client_id,
            client_name,
            client_key,
            client_type,
            permission_group,
            is_super_user,
            last_contact: -1.0, // This means that client didn't make any contact
            max_sub_channels,
            owned_sub_channels_keys,
            sub_channels_in_use: 0u32,
            client_handlers,
            syncronized: false,
        })
    }

    pub fn get_client_name(&self) -> String {
        self.client_name.clone()
    }

    pub fn save_into_db(&self) {
        let serialzied_owned_sub_channels_keys =
            serde_json::to_string(&self.owned_sub_channels_keys)
                .expect("Failed to serialize to JSON");

        with_connection!(SQL_POOL, |conn: &rusqlite::Connection| {
            // let now = Utc::now();
            // let timestamp = now.timestamp() as f64 + (now.timestamp_subsec_millis() as f64 / 1000.0);

            let client_handlers;

            if self.client_handlers.is_empty() {
                client_handlers = "".to_string();
            } else {
                // Try to convert Vec<HashMap<String, Value>> to string
                client_handlers =
                    to_string_pretty(&self.client_handlers).expect("Failed to serialize");
            }

            let result = conn.execute(
                "INSERT INTO Clients (ID, ClientName, ClientKey, ClientType, PermissionGroup, SuperUser, LastContact, MaxSubChannels, OwnedSubChannelsKeys, SubChannelsInUse, Handlers, Syncronized) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
                params![
                    self.client_id,
                    self.client_name,
                    self.client_key,
                    self.client_type,
                    self.permission_group,
                    self.is_super_user,
                    self.last_contact,
                    self.max_sub_channels,
                    serialzied_owned_sub_channels_keys,
                    self.sub_channels_in_use,
                    client_handlers,
                    self.syncronized
                ],
            );

            match result {
                Ok(rows) => {
                    if rows > 0 {
                        println!("Successfully inserted Client in the table Clients. {} row(s) were affected.", rows);
                    }
                }
                Err(e) => {
                    eprintln!(
                        "An error occurred while inserting the Client in the table Clients: {}",
                        e
                    );
                }
            };
        })
    }

    pub fn update_to(&self, new_client: &Client) -> Result<Self, ClientError> {
        let serialized_owned_sub_channels_keys =
            serde_json::to_string(&new_client.owned_sub_channels_keys)
                .expect("Failed to serialize to JSON");

        let _ = with_connection!(SQL_POOL, |conn: &rusqlite::Connection| {
            // let now = Utc::now();
            // let timestamp = now.timestamp() as f64 + (now.timestamp_subsec_millis() as f64 / 1000.0);

            let client_handlers =
                to_string_pretty(&self.client_handlers).expect("Failed to serialize"); // Try to convert Vec<HashMap<String, Value>> to string

            let result = conn.execute(
                "UPDATE Clients SET ClientName = ?, ClientKey = ?, ClientType = ?, PermissionGroup = ?, SuperUser = ?, LastContact = ?, MaxSubChannels = ?, OwnedSubChannelsKeys = ?, SubChannelsInUse = ?, Handlers = ?, Syncronized = ? WHERE ID = ?",
                params![
                    new_client.client_name,
                    new_client.client_key,
                    new_client.client_type,
                    new_client.permission_group,
                    new_client.is_super_user,
                    new_client.last_contact,
                    new_client.max_sub_channels,
                    serialized_owned_sub_channels_keys,
                    new_client.sub_channels_in_use,
                    client_handlers,
                    new_client.syncronized,
                    self.client_id,
                ],
            );

            match result {
                Ok(rows) => {
                    if rows > 0 {
                        println!("Successfully inserted Log in the table Clients. {} row(s) were affected.", rows);
                    } else {
                        println!("No rows were affected.");
                    }
                }
                Err(e) => {
                    eprintln!(
                        "An error occurred while inserting the Log in the table Clients: {}",
                        e
                    );
                }
            };
        });

        Ok(Self {
            client_id: new_client.client_id,
            client_name: new_client.client_name.clone(),
            client_key: new_client.client_key.clone(),
            client_type: new_client.client_type.clone(),
            permission_group: new_client.permission_group.clone(),
            is_super_user: new_client.is_super_user,
            last_contact: new_client.last_contact,
            max_sub_channels: new_client.max_sub_channels,
            owned_sub_channels_keys: new_client.owned_sub_channels_keys.clone(),
            sub_channels_in_use: new_client.sub_channels_in_use,
            client_handlers: new_client.client_handlers.clone(),
            syncronized: new_client.syncronized,
        })
    }

    pub fn get_by_name(client_name: &String) -> Result<Self, ClientError> {
        with_connection!(SQL_POOL, |conn: &rusqlite::Connection| {
            let mut clients: Vec<Client> = Vec::new();

            {
                let mut smtp = conn
                    .prepare("SELECT * FROM Clients WHERE ClientName = ?")
                    .unwrap();

                let clients_iter = smtp
                    .query_map(params![client_name], |row| {
                        Ok(Client::from(
                            row.get(0).unwrap(),
                            row.get(1).unwrap(),
                            row.get(2).unwrap(),
                            row.get(3).unwrap(),
                            row.get(4).unwrap(),
                            row.get(5).unwrap(),
                            row.get(6).unwrap(),
                            row.get(7).unwrap(),
                            serde_json::from_str::<Vec<String>>(row.get::<_, String>(8)?.as_str())
                                .unwrap(),
                            row.get(9).unwrap(),
                            serde_json::from_str::<Vec<HashMap<String, Value>>>(
                                row.get::<_, String>(10)?.as_str(),
                            )
                            .unwrap(),
                            row.get(11).unwrap(),
                        ))
                    })
                    .unwrap();

                for client in clients_iter {
                    clients.push(client.unwrap()?);
                }
            }

            if clients.len() == 0 {
                return Err(ClientError::ClientDoesNotExist(client_name.clone()));
            } else {
                return Ok(clients[0].clone());
            }
        })
    }

    pub fn get_by_key(client_key: &String) -> Result<Self, ClientError> {
        with_connection!(SQL_POOL, |conn: &rusqlite::Connection| {
            let mut clients: Vec<Client> = Vec::new();

            {
                let mut smtp = conn
                    .prepare("SELECT * FROM Clients WHERE ClientKey = ?")
                    .unwrap();

                let clients_iter = smtp
                    .query_map(params![*client_key], |row| {
                        Ok(Client::from(
                            row.get(0).unwrap(),
                            row.get(1).unwrap(),
                            row.get(2).unwrap(),
                            row.get(3).unwrap(),
                            row.get(4).unwrap(),
                            row.get(5).unwrap(),
                            row.get(6).unwrap(),
                            row.get(7).unwrap(),
                            serde_json::from_str::<Vec<String>>(row.get::<_, String>(8)?.as_str())
                                .unwrap(),
                            row.get(9).unwrap(),
                            serde_json::from_str::<Vec<HashMap<String, Value>>>(
                                row.get::<_, String>(10)?.as_str(),
                            )
                            .unwrap(),
                            row.get(11).unwrap(),
                        ))
                    })
                    .unwrap();

                for client in clients_iter {
                    clients.push(client.unwrap()?);
                }
            }

            if clients.len() == 0 {
                return Err(ClientError::ClientDoesNotExist(client_key.clone()));
            } else {
                return Ok(clients[0].clone());
            }
        })
    }

    pub fn delete_all() -> Result<(), ClientError> {
        let _ = with_connection!(SQL_POOL, |conn: &rusqlite::Connection| {
            let result = conn.execute("DELETE FROM Clients", params![]);

            match result {
                Ok(rows) => {
                    println!("Successfully deleted all clients from clients table! {} Rows were affected.", rows);
                }
                Err(e) => {
                    eprintln!("An error occurred while deleting all clients from clients table! And the error was: {}", e);
                }
            }
        });

        Ok(())
    }

    pub fn delete(&self) -> Result<(), ClientError> {
        if !check_if_client_key_exists(self.client_key.clone()) {
            return Err(ClientError::ClientDoesNotExist(self.client_key.clone()));
        }

        let _ = with_connection!(SQL_POOL, |conn: &rusqlite::Connection| {
            let result = conn.execute(
                "DELETE from Clients WHERE ClientKey = ?",
                params![self.client_key],
            );

            match result {
                Ok(rows) => {
                    println!(
                        "Successfully deleted Client: {} from clients! {} Rows were affected.",
                        self.client_key, rows
                    );
                }
                Err(e) => {
                    eprintln!("An error occurred while deleting Client: {} from clients! And the error was: {}", self.client_key, e);
                }
            }
        });

        Ok(())
    }

    pub fn update_last_contact(&self) -> Result<Self, ClientError> {
        let now = SystemTime::now();
        let duration_since_epoch = now.duration_since(UNIX_EPOCH).expect("Time went backwards");

        let seconds = duration_since_epoch.as_secs() as f64;
        let subsec_nanos = duration_since_epoch.subsec_nanos() as f64;

        let total_seconds = seconds + subsec_nanos * 1e-9; // Convert nanoseconds to seconds and add to the total

        let last_contact = total_seconds.clone();

        let new_client = Self {
            client_id: self.client_id.clone(),
            client_name: self.client_name.clone(),
            client_key: self.client_key.clone(),
            client_type: self.client_type.clone(),
            permission_group: self.permission_group.clone(),
            is_super_user: self.is_super_user.clone(),
            last_contact: last_contact,
            max_sub_channels: self.max_sub_channels.clone(),
            owned_sub_channels_keys: self.owned_sub_channels_keys.clone(),
            sub_channels_in_use: self.sub_channels_in_use.clone(),
            client_handlers: self.client_handlers.clone(),
            syncronized: self.syncronized.clone(),
        };

        edit_client(new_client.clone());

        println!("Update client contact for client: {}!", self.client_id);

        Ok(new_client)
    }

    pub fn update_handlers(
        &self,
        new_handlers: Vec<HashMap<String, Value>>,
    ) -> Result<Self, ClientError> {
        let new_client = Self {
            client_id: self.client_id.clone(),
            client_name: self.client_name.clone(),
            client_key: self.client_key.clone(),
            client_type: self.client_type.clone(),
            permission_group: self.permission_group.clone(),
            is_super_user: self.is_super_user.clone(),
            last_contact: self.last_contact.clone(),
            max_sub_channels: self.max_sub_channels.clone(),
            owned_sub_channels_keys: self.owned_sub_channels_keys.clone(),
            sub_channels_in_use: self.sub_channels_in_use.clone(),
            client_handlers: new_handlers,
            syncronized: self.syncronized.clone(),
        };

        edit_client(new_client.clone());

        Ok(new_client)
    }

    pub fn is_sync(&self) -> bool {
        self.syncronized
    }

    pub fn change_sync_to(&self, sync: bool) -> Result<Self, ClientError> {
        let new_client = Self {
            client_id: self.client_id.clone(),
            client_name: self.client_name.clone(),
            client_key: self.client_key.clone(),
            client_type: self.client_type.clone(),
            permission_group: self.permission_group.clone(),
            is_super_user: self.is_super_user.clone(),
            last_contact: self.last_contact.clone(),
            max_sub_channels: self.max_sub_channels.clone(),
            owned_sub_channels_keys: self.owned_sub_channels_keys.clone(),
            sub_channels_in_use: self.sub_channels_in_use.clone(),
            client_handlers: self.client_handlers.clone(),
            syncronized: sync,
        };

        edit_client(new_client.clone());

        Ok(new_client)
    }

    pub fn change_key_to(&self, new_client_key: String) -> Result<Self, ClientError> {
        if !check_if_client_key_exists(self.client_key.clone()) {
            return Err(ClientError::ClientDoesNotExist(self.client_key.clone()));
        }

        let new_client = Self {
            client_id: self.client_id,
            client_name: self.client_name.clone(),
            client_key: new_client_key,
            client_type: self.client_type.clone(),
            permission_group: self.permission_group.clone(),
            is_super_user: self.is_super_user.clone(),
            last_contact: self.last_contact,
            max_sub_channels: self.max_sub_channels,
            owned_sub_channels_keys: self.owned_sub_channels_keys.clone(),
            sub_channels_in_use: self.sub_channels_in_use,
            client_handlers: self.client_handlers.clone(),
            syncronized: self.syncronized.clone(),
        };

        edit_client(new_client.clone());

        Ok(new_client)
    }

    fn from(
        client_id: u32,
        client_name: String,
        client_key: String,
        client_type: String,
        permission_group: String,
        is_super_user: bool,
        last_contact: f64,
        max_sub_channels: u32,
        owned_sub_channels_keys: Vec<String>,
        sub_channels_in_use: u32,
        client_handlers: Vec<HashMap<String, Value>>,
        syncronized: bool,
    ) -> Result<Self, ClientError> {
        Ok(Self {
            client_id,
            client_name,
            client_key,
            client_type,
            permission_group,
            is_super_user,
            last_contact,
            max_sub_channels,
            owned_sub_channels_keys,
            sub_channels_in_use,
            client_handlers,
            syncronized,
        })
    }
}

pub fn registry_new_client(
    client_name: String,
    client_key: String,
    client_type: String,
    client_permission_group: String,
    client_is_super_user: bool,
    client_max_sub_channels: u32,
    client_owned_sub_channels_keys: Vec<String>,
    client_handlers: Vec<HashMap<String, Value>>,
) {
    if check_if_client_key_exists(client_key.clone()) {
        return;
    }

    //> TYPES

    // TODO >>> Create a enum for client Type be more organized

    //> VERIFICATION:

    // TODO >>> Verify if the client permission group exists
    //* Also maybe will be nice to make a way to be able to retrieve the valid permission groups

    // TODO >>> See if the max sub channels value is a valid number
    // TODO >>> See if the owned sub channel keys are valid ones

    let client = handle_manager_client_error!(Client::new(
        client_name.clone(),
        client_key.clone(),
        client_type,
        client_permission_group,
        client_is_super_user,
        client_max_sub_channels,
        client_owned_sub_channels_keys,
        client_handlers,
    ));

    client.save_into_db();
}

pub fn check_if_client_key_exists(client_key: String) -> bool {
    let client_keys: Vec<String> = get_clients_keys_registered();

    if client_keys.contains(&client_key) {
        return true;
    } else {
        return false;
    }
}

fn get_clients_keys_registered() -> Vec<String> {
    with_connection!(SQL_POOL, |conn: &rusqlite::Connection| {
        let mut keys: Vec<String> = Vec::new();

        {
            let mut smtp: Statement<'_> = conn.prepare("SELECT * FROM Clients").unwrap();
            let keys_iter = smtp
                .query_map(params![], |row: &Row<'_>| {
                    let key: String = row.get(2)?;
                    Ok(key)
                })
                .unwrap();

            for key in keys_iter {
                keys.push(key.unwrap());
            }
        }

        // println!("Client Keys registred: {:?}", keys.clone());

        keys
    })
}

pub fn get_all_clients() -> Result<Vec<Client>, ClientError> {
    let mut clients: Vec<Client> = Vec::new();

    with_connection!(SQL_POOL, |conn: &rusqlite::Connection| {
        let mut keys: Vec<String> = Vec::new();

        {
            let mut smtp: Statement<'_> = conn.prepare("SELECT * FROM Clients").unwrap();
            let clients_iter = smtp
                .query_map(params![], |row: &Row<'_>| {
                    Ok(Client::from(
                        row.get(0).unwrap(),
                        row.get(1).unwrap(),
                        row.get(2).unwrap(),
                        row.get(3).unwrap(),
                        row.get(4).unwrap(),
                        row.get(5).unwrap(),
                        row.get(6).unwrap(),
                        row.get(7).unwrap(),
                        serde_json::from_str::<Vec<String>>(row.get::<_, String>(8)?.as_str())
                            .unwrap(),
                        row.get(9).unwrap(),
                        serde_json::from_str::<Vec<HashMap<String, Value>>>(
                            row.get::<_, String>(10)?.as_str(),
                        )
                        .unwrap(),
                        row.get(11).unwrap(),
                    ))
                })
                .unwrap();

            for client in clients_iter {
                clients.push(client.unwrap()?);
            }

            if clients.len() == 0 {
                return Err(ClientError::UnexpectedError(
                    "Any clients registred!".to_string(),
                ));
            } else {
                return Ok(clients.clone());
            }
        }
    })
}

fn get_registered_ids() -> Vec<u32> {
    with_connection!(SQL_POOL, |conn: &rusqlite::Connection| {
        let mut ids: Vec<u32> = Vec::new();

        {
            let mut smtp = conn.prepare("SELECT * FROM Clients").unwrap();
            let commands_iter = smtp
                .query_map(params![], |row| {
                    let id: u32 = row.get(0)?;
                    Ok(id)
                })
                .unwrap();

            for command in commands_iter {
                ids.push(command.unwrap());
            }
        }

        ids
    })
}

pub fn edit_client(client: Client) {
    with_connection!(SQL_POOL, |conn: &rusqlite::Connection| {
        let serialized_owned_sub_channels_keys =
            serde_json::to_string(&client.owned_sub_channels_keys)
                .expect("Failed to serialize to JSON");

        let client_handlers =
            to_string_pretty(&client.client_handlers).expect("Failed to serialize"); // Try to convert Vec<HashMap<String, Value>> to string

        let result = conn.execute(
            "UPDATE Clients SET ClientName = ?, ClientKey = ?, ClientType = ?, PermissionGroup = ?, SuperUser = ?, LastContact = ?, MaxSubChannels = ?, OwnedSubChannelsKeys = ?, SubChannelsInUse = ?, Handlers = ? WHERE ID = ?;",
            params![
                client.client_name,
                client.client_key,
                client.client_type,
                client.permission_group,
                client.is_super_user,
                client.last_contact,
                client.max_sub_channels,
                serialized_owned_sub_channels_keys,
                client.sub_channels_in_use,
                client_handlers,
                client.client_id,
            ],
        );

        match result {
            Ok(rows) => {
                if rows > 0 {
                    println!(
                        "Successfully update client: {} in database",
                        client.client_name
                    );
                }
            }
            Err(e) => {
                eprintln!(
                    "Error while update client: {} in the database, the error is: {}",
                    client.client_name, e
                );
            }
        }
    });
}
