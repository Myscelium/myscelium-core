use lazy_static::lazy_static;
use serde_json::to_string;

use crate::common::types::SchedulingError;
use crate::socket_client::transposer::ProcessError;
use crate::ClientLoaderError;
#[macro_use]
use crate::{with_connection, set_new_path_to_buffer_db};
use crate::common::sql_pool::pool::{SQLiteConnectionPool, UniqueParityIdGenerator};

use rusqlite::params;
use serde_json::{from_str, to_string_pretty, Value};

use core::fmt;
use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::Mutex;

use rusqlite::{Connection, Result};

use std::thread;
use std::time::Duration;

use rusqlite::Row;
use rusqlite::Statement;

use std::time::SystemTime;
use std::time::UNIX_EPOCH;

// #[macro_export]
// macro_rules! handle_manager_client_error {
//     ($client_result:expr) => {
//         match $client_result {
//             Ok(c) => c, // Return the unwrapped client directly
//             Err(e) => {
//                 match e {
//                     ClientError::ClientAlreadyExist(c) => {
//                         println!("Error client: {} already exist", c);
//                     },
//                     ClientError::ClientDoesNotExist(c) => {
//                         println!("Error client: {} doesn't exist", c);
//                     },
//                     ClientError::UnexpectedError(e) => {
//                         println!("Get a unexpected error: {}", e);
//                     },
//                     _ => {
//                         println!("Get a unexpected error!");
//                     },
//                 }
//                 panic!("Client error encountered!"); // Panic after printing the error
//             },
//         }
//     };
// }

lazy_static! {
    static ref SQL_NAME: Arc<Mutex<String>> = Arc::new(Mutex::new("Data.db".to_string()));
    static ref SQL_PATH: Arc<Mutex<String>> = Arc::new(Mutex::new("Data.db".to_string()));
    static ref NUM_WORKERS: Arc<Mutex<u32>> = Arc::new(Mutex::new(5));
    static ref SQL_POOL: Arc<Mutex<SQLiteConnectionPool>> = Arc::new(Mutex::new(SQLiteConnectionPool::empty()));
}

pub async fn set_host_clients_manager__pool_workers_num(n_workers: u32) {
    let mut default_num_of_workers = NUM_WORKERS.lock().await;
    *default_num_of_workers = n_workers;
}

pub async fn clients_manager_initialize_table(sql_path: String) {
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

    let fut = set_new_path_to_buffer_db!(SQL_POOL, NUM_WORKERS, sql_path, SQL_NAME);
    fut.await;

    with_connection!(SQL_POOL, |conn: rusqlite::Connection| async {
        let result = conn.execute(
            "CREATE TABLE IF NOT EXISTS Clients (ID INTEGER PRIMARY KEY AUTOINCREMENT, ClientName TEXT, ClientKey TEXT, ClientType TEXT, PermissionGroup TEXT, SuperUser BOOL, LastContact NUMBER, MaxSubChannels NUMBER, OwnedSubChannelsKeys TEXT, SubChannelsInUse NUMBER, Handlers TEXT, Syncronized BOOL)",
            params![],
        );

        match result {
            Ok(_) => {
                println!("Successfully initialize Clients table!");
            },
            Err(e) => {
                eprintln!("An error occurred while scheduling the command in the Clients table: {}", e);
            },
        };

        ((), conn)
    }).await;
}

#[derive(Debug, Clone)]
pub enum ClientError {
    ClientDoesNotExist(String),
    ClientAlreadyExist(String),
    UnexpectedError(String),
    InvalidCommand(String),
    ClientIsNotRunning(String),
    ClientIsNotFullyInitialized(String),
    NotAbleToReadClientStates,
    TargetDoesntExists(String),
    HandlerDoesntExist(String),
    ResponseHandlerDoesntExist(String),
    CantScheduleCommandsToItself,
    HostCantSendResponseToItself,
    TargetCantSendResponseToItself,
    BufferError(String),
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClientError::ClientDoesNotExist(name) => write!(f, "Client does not exist: {}", name),
            ClientError::ClientAlreadyExist(name) => write!(f, "Client already exists: {}", name),
            ClientError::UnexpectedError(msg) => write!(f, "Unexpected error: {}", msg),
            ClientError::InvalidCommand(cmd) => write!(f, "Invalid command: {}", cmd),

            ClientError::ClientIsNotRunning(c) => write!(f, "Client {:?} is not running", c),
            ClientError::ClientIsNotFullyInitialized(c) => write!(f, "Client: {:?} is not fully initialized", c),
            ClientError::NotAbleToReadClientStates => write!(f, "Unable to read client states"),
            ClientError::TargetDoesntExists(t) => write!(f, "Target: {:?} does not exist", t),
            ClientError::HandlerDoesntExist(h) => write!(f, "Handler: {:?} does not exist", h),
            ClientError::ResponseHandlerDoesntExist(rh) => write!(f, "Response: {:?} handler does not exist", rh),
            ClientError::CantScheduleCommandsToItself => write!(f, "Cannot schedule commands to itself"),
            ClientError::HostCantSendResponseToItself => write!(f, "Host cannot send response to itself"),
            ClientError::TargetCantSendResponseToItself => write!(f, "Target cannot send response to itself"),

            ClientError::BufferError(err) => write!(f, "Buffer error: {}", err),
        }
    }
}

// Faster converter to simplify the error possibilities in unreachable variant scenarios.
impl From<ClientError> for ClientLoaderError {
    fn from(value: ClientError) -> Self {
        match value {
            ClientError::ClientDoesNotExist(client) => ClientLoaderError::ClientDoesNotExist(client),
            ClientError::ClientAlreadyExist(client) => ClientLoaderError::ClientAlreadyExist(client),
            ClientError::UnexpectedError(e) => ClientLoaderError::UnexpectedError(e),
            ClientError::NotAbleToReadClientStates => ClientLoaderError::NotAbleToReadClientStates,
            ClientError::ClientIsNotRunning(_) => unreachable!("ClientError::ClientIsNotRunning should never be converted into ClientLoaderError!"),
            ClientError::InvalidCommand(e) => unreachable!("ClientError::InvalidCommand ({:?}) should never be converted into ClientLoaderError!", e),
            ClientError::ClientIsNotFullyInitialized(_) => unreachable!("ClientError::ClientNotFullyInitialized should never be converted into ClientLoaderError!"),
            ClientError::TargetDoesntExists(_) => unreachable!("ClientError::TargetDoesntExists should never be converted into ClientLoaderError!"),
            ClientError::HandlerDoesntExist(_) => unreachable!("ClientError::HandlerDoesntExist should never be converted into ClientLoaderError!"),
            ClientError::ResponseHandlerDoesntExist(_) => unreachable!("ClientError::ResponseHandlerDoesntExist should never be converted into ClientLoaderError!"),
            ClientError::CantScheduleCommandsToItself => unreachable!("ClientError::CantScheduleCommandsToItself should never be converted into ClientLoaderError!"),
            ClientError::HostCantSendResponseToItself => unreachable!("ClientError::HostCantSendResponseToItself should never be converted into ClientLoaderError!"),
            ClientError::TargetCantSendResponseToItself => unreachable!("ClientError::TargetCantSendResponseToItself should never be converted into ClientLoaderError!"),
            ClientError::BufferError(e) => unreachable!("ClientError::BufferError {:?} should never be converted into ClientLoaderError!", e),
        }
    }
}

impl From<SchedulingError> for ClientError {
    fn from(value: SchedulingError) -> Self {
        match value {
            SchedulingError::CantReadStates => {
                return ClientError::NotAbleToReadClientStates;
            },
            SchedulingError::ClientIsntFullyInitialized(c) => {
                return ClientError::ClientIsNotFullyInitialized(c);
            },
            SchedulingError::CantScheduleCommandsToItself(c) => return ClientError::ClientIsNotFullyInitialized(c),
            SchedulingError::HandlerDoesntExist(h) => return ClientError::HandlerDoesntExist(h),
            SchedulingError::HostCantSendResponseToItself => return ClientError::HostCantSendResponseToItself,
            SchedulingError::ResponseHandlerDoesntExist(r) => return ClientError::ResponseHandlerDoesntExist(r),
            SchedulingError::TargetCantSendResponseToItself => return ClientError::TargetCantSendResponseToItself,
            SchedulingError::TargetDoesntExists(t) => return ClientError::TargetDoesntExists(t),
            SchedulingError::UnsuportedAction(a) => return ClientError::InvalidCommand(a),
            SchedulingError::BufferError(e) => return ClientError::BufferError(e),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Client {
    pub client_id: Option<u32>,
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

            client_handlers = serde_json::from_str::<Vec<HashMap<String, Value>>>(json_str).unwrap();
            // Using unwrap because the source is controlled!
        }

        Ok(Self {
            client_id: None,
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

    pub async fn exists_in_db(&self) -> Result<bool, ClientError> {
        with_connection!(SQL_POOL, |conn: rusqlite::Connection| async {
            let result = conn.query_row("SELECT 1 FROM Clients WHERE ClientName = ? LIMIT 1", params![self.client_name], |_row| Ok(()));

            let result = match result {
                Ok(_) => Ok(true),                                      // Found a row
                Err(rusqlite::Error::QueryReturnedNoRows) => Ok(false), // No match
                Err(e) => Err(ClientError::UnexpectedError(format!("Error checking client existence: {:?}", e))),
            };

            (result, conn)
        })
        .await
    }

    pub async fn save_into_db(&self) -> Result<(), ClientError> {
        let serialzied_owned_sub_channels_keys = serde_json::to_string(&self.owned_sub_channels_keys).expect("Failed to serialize to JSON");

        if self.exists_in_db().await? {
            self.update_to(self).await?;
            return Ok(());
        }

        with_connection!(SQL_POOL, |conn: rusqlite::Connection| async {
            // let now = Utc::now();
            // let timestamp = now.timestamp() as f64 + (now.timestamp_subsec_millis() as f64 / 1000.0);

            let client_handlers;

            if self.client_handlers.is_empty() {
                client_handlers = "".to_string();
            } else {
                // Try to convert Vec<HashMap<String, Value>> to string
                client_handlers = to_string_pretty(&self.client_handlers).expect("Failed to serialize");
            }

            let result = conn.execute(
                "INSERT INTO Clients (ClientName, ClientKey, ClientType, PermissionGroup, SuperUser, LastContact, MaxSubChannels, OwnedSubChannelsKeys, SubChannelsInUse, Handlers, Syncronized) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
                params![
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

            let result = match result {
                Ok(rows) => {
                    if rows > 0 {
                        println!("Successfully inserted Client in the table Clients. {} row(s) were affected.", rows);
                        Ok(())
                    } else {
                        Err(ClientError::UnexpectedError("No rows were affected, client wasn't properly inserted into the database!".to_string()))
                    }
                },
                Err(e) => Err(ClientError::UnexpectedError(format!("An error occurred while inserting the Client in the table Clients: {}", e))),
            };

            (result, conn)
        })
        .await
    }

    pub async fn update_to(&self, new_client: &Client) -> Result<Self, ClientError> {
        let serialized_owned_sub_channels_keys = serde_json::to_string(&new_client.owned_sub_channels_keys).expect("Failed to serialize to JSON");

        let _ = with_connection!(SQL_POOL, |conn: rusqlite::Connection| async {
            // let now = Utc::now();
            // let timestamp = now.timestamp() as f64 + (now.timestamp_subsec_millis() as f64 / 1000.0);

            let client_handlers = to_string_pretty(&self.client_handlers).expect("Failed to serialize"); // Try to convert Vec<HashMap<String, Value>> to string

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
                },
                Err(e) => {
                    eprintln!("An error occurred while inserting the Log in the table Clients: {}", e);
                },
            };

            ((), conn)
        }).await;

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

    pub async fn get_by_name(client_name: &String) -> Result<Self, ClientError> {
        with_connection!(SQL_POOL, |conn: rusqlite::Connection| async {
            let clients: Result<Vec<Client>, ClientError> = 'loading: {
                let mut smtp = match conn.prepare("SELECT * FROM Clients WHERE ClientName = ?") {
                    Ok(s) => s,
                    Err(e) => break 'loading Err(ClientError::UnexpectedError(format!("prepare failed: {e}"))),
                };

                let mut clients: Vec<Client> = Vec::new();
                let clients_iter = match smtp.query_map(params![client_name], |row| {
                    Ok(Client::from(
                        row.get(0).unwrap(),
                        row.get(1).unwrap(),
                        row.get(2).unwrap(),
                        row.get(3).unwrap(),
                        row.get(4).unwrap(),
                        row.get(5).unwrap(),
                        row.get(6).unwrap(),
                        row.get(7).unwrap(),
                        serde_json::from_str::<Vec<String>>(row.get::<_, String>(8)?.as_str()).unwrap(),
                        row.get(9).unwrap(),
                        serde_json::from_str::<Vec<HashMap<String, Value>>>(row.get::<_, String>(10)?.as_str()).unwrap(),
                        row.get(11).unwrap(),
                    ))
                }) {
                    Ok(clients) => clients,
                    Err(e) => {
                        break 'loading Err(ClientError::UnexpectedError(format!("Query map error: {}", e)));
                    },
                };

                for client_result in clients_iter {
                    match client_result {
                        Ok(client) => match client {
                            Ok(c) => clients.push(c),
                            Err(e) => break 'loading Err(e),
                        },
                        Err(e) => {
                            break 'loading Err(ClientError::UnexpectedError(format!("Client row parse error: {}", e)));
                        },
                    }
                }

                Ok(clients)
            };

            let result: Result<Client, ClientError> = match clients {
                Ok(mut clients) => {
                    if clients.is_empty() {
                        Err(ClientError::ClientDoesNotExist(client_name.clone()))
                    } else {
                        Ok(clients.remove(0))
                    }
                },
                Err(e) => Err(e),
            };

            (result, conn)
        })
        .await
    }

    pub async fn get_by_key(client_key: &String) -> Result<Self, ClientError> {
        with_connection!(SQL_POOL, |conn: rusqlite::Connection| async {
            let clients: Result<Vec<Client>, ClientError> = 'get: {
                let mut clients: Vec<Client> = Vec::new();
                let mut smtp = match conn.prepare("SELECT * FROM Clients WHERE ClientKey = ?") {
                    Ok(s) => s,
                    Err(e) => break 'get Err(ClientError::UnexpectedError(format!("prepare failed: {e}"))),
                };

                let clients_iter = match smtp.query_map(params![*client_key], |row| {
                    Ok(Client::from(
                        row.get(0).unwrap(),
                        row.get(1).unwrap(),
                        row.get(2).unwrap(),
                        row.get(3).unwrap(),
                        row.get(4).unwrap(),
                        row.get(5).unwrap(),
                        row.get(6).unwrap(),
                        row.get(7).unwrap(),
                        serde_json::from_str::<Vec<String>>(row.get::<_, String>(8)?.as_str()).unwrap(),
                        row.get(9).unwrap(),
                        serde_json::from_str::<Vec<HashMap<String, Value>>>(row.get::<_, String>(10)?.as_str()).unwrap(),
                        row.get(11).unwrap(),
                    ))
                }) {
                    Ok(i) => i,
                    Err(e) => break 'get Err(ClientError::UnexpectedError(format!("query_map failed: {e}"))),
                };

                for client in clients_iter {
                    match client.map_err(|e| ClientError::UnexpectedError(format!("Error retrieving clients from key: {:?}", e))) {
                        Ok(c) => match c {
                            Ok(c) => clients.push(c),
                            Err(e) => break 'get Err(e),
                        },
                        Err(e) => break 'get Err(e),
                    };
                }

                break 'get Ok(clients);
            };

            let result: Result<Client, ClientError> = 'compute_result: {
                // clients is a Result<Vec<Client>, ClientError>
                let clients = match clients {
                    Ok(c) => c,
                    Err(e) => break 'compute_result Err(e), // Exit just this block
                };
                if clients.is_empty() {
                    break 'compute_result Err(ClientError::ClientDoesNotExist(client_key.clone()));
                    // compute_result is the label name for that block!
                }

                // normal path
                break 'compute_result Ok(clients[0].clone());
            };

            (result, conn)
        })
        .await
    }

    pub async fn delete_all() -> Result<(), ClientError> {
        let _ = with_connection!(SQL_POOL, |conn: rusqlite::Connection| async {
            let result = conn.execute("DELETE FROM Clients", params![]);
            match result {
                Ok(rows) => {
                    println!("Successfully deleted all clients from clients table! {} Rows were affected.", rows);
                },
                Err(e) => {
                    eprintln!("An error occurred while deleting all clients from clients table! And the error was: {}", e);
                },
            }

            ((), conn)
        })
        .await;

        Ok(())
    }

    pub async fn delete(&self) -> Result<(), ClientError> {
        if !check_if_client_key_exists(self.client_key.clone()).await? {
            return Err(ClientError::ClientDoesNotExist(self.client_key.clone()));
        }

        let _ = with_connection!(SQL_POOL, |conn: rusqlite::Connection| async {
            let result = conn.execute("DELETE from Clients WHERE ClientKey = ?", params![self.client_key]);

            match result {
                Ok(rows) => {
                    println!("Successfully deleted Client: {} from clients! {} Rows were affected.", self.client_key, rows);
                },
                Err(e) => {
                    eprintln!("An error occurred while deleting Client: {} from clients! And the error was: {}", self.client_key, e);
                },
            }

            ((), conn)
        })
        .await;

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

        // println!("Update client contact for client: {}!", self.client_id);

        Ok(new_client)
    }

    pub fn update_handlers(&self, new_handlers: Vec<HashMap<String, Value>>) -> Result<Self, ClientError> {
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

    pub async fn change_sync_to(&self, sync: bool) -> Result<Self, ClientError> {
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

        edit_client(new_client.clone()).await;

        Ok(new_client)
    }

    pub async fn change_key_to(&self, new_client_key: String) -> Result<Self, ClientError> {
        if !check_if_client_key_exists(self.client_key.clone()).await? {
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
            client_id: Some(client_id),
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

pub async fn registry_new_client(
    client_name: String,
    client_key: String,
    client_type: String,
    client_permission_group: String,
    client_is_super_user: bool,
    client_max_sub_channels: u32,
    client_owned_sub_channels_keys: Vec<String>,
    client_handlers: Vec<HashMap<String, Value>>,
) -> Result<(), ClientError> {
    if check_if_client_key_exists(client_key.clone()).await? {
        return Err(ClientError::ClientAlreadyExist(client_key));
    }

    //> TYPES

    // TODO >>> Create a enum for client Type be more organized

    //> VERIFICATION:

    // TODO >>> Verify if the client permission group exists
    //* Also maybe will be nice to make a way to be able to retrieve the valid permission groups

    // TODO >>> See if the max sub channels value is a valid number
    // TODO >>> See if the owned sub channel keys are valid ones

    let client = Client::new(
        client_name.clone(),
        client_key.clone(),
        client_type,
        client_permission_group,
        client_is_super_user,
        client_max_sub_channels,
        client_owned_sub_channels_keys,
        client_handlers,
    )?;

    client.save_into_db().await;
    Ok(())
}

pub async fn check_if_client_key_exists(client_key: String) -> Result<bool, ClientError> {
    let client_keys: Vec<String> = get_clients_keys_registered().await?;

    if client_keys.contains(&client_key) {
        return Ok(true);
    } else {
        return Ok(false);
    }
}

async fn get_clients_keys_registered() -> Result<Vec<String>, ClientError> {
    with_connection!(SQL_POOL, |conn: rusqlite::Connection| async {
        let keys: Result<Vec<String>, ClientError> = 'load: {
            let mut smtp: Statement<'_> = match conn.prepare("SELECT * FROM Clients") {
                Ok(s) => s,
                Err(e) => break 'load Err(ClientError::UnexpectedError(format!("prepare failed: {e}"))),
            };

            let keys_iter = match smtp.query_map(params![], |row: &Row<'_>| {
                let key: String = row.get(2)?;
                Ok(key)
            }) {
                Ok(k) => k,
                Err(e) => break 'load Err(ClientError::UnexpectedError(format!("Error trying to load the client keys registered, the error was: {:?}", e))),
            };

            let mut keys: Vec<String> = Vec::new();
            for key in keys_iter {
                match key {
                    Ok(k) => keys.push(k),
                    Err(e) => break 'load Err(ClientError::UnexpectedError(format!("Error trying to load the client keys registered, the error was: {:?}", e))),
                }
            }

            Ok(keys)
        };

        // println!("Client Keys registred: {:?}", keys.clone());

        (keys, conn)
    })
    .await
}

pub async fn get_all_clients() -> Result<Vec<Client>, ClientError> {
    with_connection!(SQL_POOL, |conn: rusqlite::Connection| async {
        let clients: Result<Vec<Client>, ClientError> = 'load: {
            let mut keys: Vec<String> = Vec::new();
            let mut smtp = match conn.prepare("SELECT * FROM Clients") {
                Ok(s) => s,
                Err(e) => break 'load Err(ClientError::UnexpectedError(format!("prepare failed: {e}"))),
            };

            let clients_iter = match smtp.query_map(params![], |row: &Row<'_>| {
                Ok(Client::from(
                    row.get(0).unwrap(),
                    row.get(1).unwrap(),
                    row.get(2).unwrap(),
                    row.get(3).unwrap(),
                    row.get(4).unwrap(),
                    row.get(5).unwrap(),
                    row.get(6).unwrap(),
                    row.get(7).unwrap(),
                    serde_json::from_str::<Vec<String>>(row.get::<_, String>(8)?.as_str()).unwrap(),
                    row.get(9).unwrap(),
                    serde_json::from_str::<Vec<HashMap<String, Value>>>(row.get::<_, String>(10)?.as_str()).unwrap(),
                    row.get(11).unwrap(),
                ))
            }) {
                Ok(i) => i,
                Err(e) => break 'load Err(ClientError::UnexpectedError(format!("query_map failed: {e}"))),
            };

            let mut clients: Vec<Client> = Vec::new();
            for client in clients_iter {
                match client {
                    Ok(c) => match c {
                        Ok(c) => clients.push(c),
                        Err(e) => break 'load Err(ClientError::UnexpectedError(format!("Error trying to load the client, the error is: {:?}", e))),
                    },
                    Err(e) => break 'load Err(ClientError::UnexpectedError(format!("Error trying to load the client, the error is: {:?}", e))),
                };
            }

            if clients.len() == 0 {
                break 'load Err(ClientError::UnexpectedError("Any clients registred!".to_string()));
            } else {
                break 'load Ok(clients);
            }
        };

        (clients, conn)
    })
    .await
}

async fn get_registered_ids() -> Vec<u32> {
    with_connection!(SQL_POOL, |conn: rusqlite::Connection| async {
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

        (ids, conn)
    })
    .await
}

pub async fn edit_client(client: Client) {
    with_connection!(SQL_POOL, |conn: rusqlite::Connection| async {
        let serialized_owned_sub_channels_keys = serde_json::to_string(&client.owned_sub_channels_keys).expect("Failed to serialize to JSON");

        let client_handlers = to_string_pretty(&client.client_handlers).expect("Failed to serialize"); // Try to convert Vec<HashMap<String, Value>> to string

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
                    // println!("Successfully update client: {} in database", client.client_name);
                }
            },
            Err(e) => {
                eprintln!("Error while update client: {} in the database, the error is: {}", client.client_name, e);
            },
        }

        ((), conn)
    })
    .await;
}
