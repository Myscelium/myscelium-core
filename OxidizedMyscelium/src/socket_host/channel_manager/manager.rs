use lazy_static::lazy_static;

#[macro_use]
use crate::{with_connection, set_new_path_to_buffer_db};
use crate::common::sql_pool::pool::{SQLiteConnectionPool, UniqueIdGenerator, UniqueParityIdGenerator};

use rusqlite::params;

use std::sync::Arc;

use parking_lot::Mutex;

use rusqlite::{Connection, Result};

use std::thread;
use std::time::Duration;

use rusqlite::Row;
use rusqlite::Statement;

use rusqlite::{types::ToSql, types::ValueRef};

use rusqlite::types::ToSqlOutput;

lazy_static! {
    static ref BUFFER_NAME: Arc<Mutex<String>> = Arc::new(Mutex::new("buffer.db".to_string()));
    static ref BUFFER_PATH: Arc<Mutex<String>> = Arc::new(Mutex::new("buffer.db".to_string()));
    static ref NUM_WORKERS: Arc<Mutex<u32>> = Arc::new(Mutex::new(5));
    static ref SQL_POOL: Mutex<SQLiteConnectionPool> = Mutex::new(SQLiteConnectionPool::empty());
}

pub fn set_workers_num(n_workers: u32) {
    let mut default_num_of_workers = NUM_WORKERS.lock();

    *default_num_of_workers = n_workers;
}

pub fn client_channel_mananger_initialize_table(buffer_path: String) {
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

    set_new_path_to_buffer_db!(SQL_POOL, NUM_WORKERS, buffer_path, BUFFER_NAME);

    with_connection!(SQL_POOL, |conn: &rusqlite::Connection| {
        let result = conn.execute(
            "CREATE TABLE IF NOT EXISTS Channels (ID INT PRIMARY KEY, OwnerClientKey TEXT, ChanelName TEXT, ChannelPurpose TEXT, Status TEXT, ChannelLifetime NUMBER, LastContact NUMBER, Streaming BOOL)",
            params![],
        );

        match result {
            Ok(_) => {
                println!("Successfully initialize Channels table!");
            },
            Err(e) => {
                eprintln!("An error occurred while scheduling the command in the Channels table: {}", e);
            },
        };
    });
}

#[derive(Debug, Clone)]
pub enum ChannelStatus {
    Waiting,
    Streaming,
    Sleeping,
    Dead,
}

impl ToSql for ChannelStatus {
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        let value = match self {
            ChannelStatus::Waiting => rusqlite::types::Value::Text("Waiting".to_string()),
            ChannelStatus::Streaming => rusqlite::types::Value::Text("Streaming".to_string()),
            ChannelStatus::Sleeping => rusqlite::types::Value::Text("Sleeping".to_string()),
            ChannelStatus::Dead => rusqlite::types::Value::Text("Dead".to_string()),
        };
        Ok(ToSqlOutput::Owned(value))
    }
}

#[derive(Debug, Clone)]
pub enum ChannelError {
    ChannelDoesNotExists,
    ChannelAlreadyStreaming,
    IncompatiblePurpose,
}

#[derive(Debug, Clone)]
pub enum ChannelPurpose {
    BinaryTransfer,
    BinarySignalStream,
}

impl ToSql for ChannelPurpose {
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        let value = match self {
            ChannelPurpose::BinaryTransfer => rusqlite::types::Value::Text("BinaryTransfer".to_string()),
            ChannelPurpose::BinarySignalStream => rusqlite::types::Value::Text("BinarySignalStream".to_string()),
        };
        Ok(ToSqlOutput::Owned(value))
    }
}

#[derive(Debug, Clone)]
pub struct Channel {
    channel_id: u32,
    owner_key: String,
    channel_name: String,
    channel_purpose: ChannelPurpose,
    channel_status: ChannelStatus,
    channel_lifetime: f64,
    last_contact: f64,
}

impl Channel {
    fn new(owner_key: String, channel_name: String, channel_purpose: ChannelPurpose, channel_lifetime: Duration) -> Self {
        let channel_id = 0u32;
        let channel_lifetime = 0f64;
        let last_contact = 0f64;
        Self {
            channel_id,
            owner_key,
            channel_name,
            channel_purpose,
            channel_status: ChannelStatus::Sleeping,
            channel_lifetime,
            last_contact,
        }
    }

    fn is_streaming(&self) -> bool {
        match self.channel_status {
            ChannelStatus::Streaming => true,
            _ => false,
        }
    }

    fn from(channel_id: u32, owner_key: String, channel_name: String, channel_purpose: ChannelPurpose, channel_lifetime: f64, last_contact: f64) -> Self {
        Self {
            channel_id,
            owner_key,
            channel_name,
            channel_purpose,
            channel_status: ChannelStatus::Sleeping,
            channel_lifetime,
            last_contact,
        }
    }

    fn get_last_contact(&self) -> f64 {
        self.last_contact.clone()
    }

    fn get_lifetime(&self) -> f64 {
        self.channel_lifetime.clone()
    }

    fn get_channel_by_id(channel_id: u32) {}
}

pub fn check_if_channel_key_exists(client_key: String) -> bool {
    let client_keys: Vec<String> = get_channels_keys_registered();

    if client_keys.contains(&client_key) {
        return true;
    } else {
        return false;
    }
}

fn get_channels_keys_registered() -> Vec<String> {
    with_connection!(SQL_POOL, |conn: &rusqlite::Connection| {
        let mut keys: Vec<String> = Vec::new();

        {
            let mut smtp: Statement<'_> = conn.prepare("SELECT * FROM Channels").unwrap();
            let commands_iter = smtp
                .query_map(params![], |row: &Row<'_>| {
                    let key: String = row.get(1)?;
                    Ok(key)
                })
                .unwrap();

            for command in commands_iter {
                keys.push(command.unwrap());
            }
        }

        keys
    })
}

fn get_registered_ids() -> Vec<u32> {
    with_connection!(SQL_POOL, |conn: &rusqlite::Connection| {
        let mut ids: Vec<u32> = Vec::new();

        {
            let mut smtp = conn.prepare("SELECT * FROM Channels").unwrap();
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

pub fn registry_client(channel_id: u32, owner_key: String, channel_name: String, channel_purpose: ChannelPurpose, channel_status: ChannelStatus, channel_lifetime: f64, last_contact: f64) {
    with_connection!(SQL_POOL, |conn: &rusqlite::Connection| {
        // let now = Utc::now();
        // let timestamp = now.timestamp() as f64 + (now.timestamp_subsec_millis() as f64 / 1000.0);

        let registered_ids = get_registered_ids();

        let mut id_generator = UniqueIdGenerator { registered_ids: registered_ids };

        let result = conn.execute(
            "INSERT INTO Channels (ID, OwnerClientKey, ChanelName, ChannelPurpose, Status, ChannelLifetime, LastContact, Streaming) VALUES (?, ?, ?, ?, ?, ?, ?, ?);",
            params![id_generator.gen(), owner_key, channel_name, channel_purpose, channel_status, channel_lifetime, last_contact],
        );

        match result {
            Ok(rows) => {
                if rows > 0 {
                    println!("Successfully inserted Log in the table Channels. {} row(s) were affected.", rows);
                } else {
                    println!("No rows were affected.");
                }
            },
            Err(e) => {
                eprintln!("An error occurred while inserting the Log in the table Channels: {}", e);
            },
        };
    })
}

// fn get_channels_by_key(client_key: String) -> Result<Client, ClientError> {
//     with_connection!(SQL_POOL, |conn: &rusqlite::Connection| {
//         let mut clients: Vec<Client> = Vec::new();

//         {
//             let mut smtp = conn.prepare("SELECT * FROM Clients WHERE ClientKey = ?").unwrap();

//             let clients_iter = smtp
//                 .query_map(params![client_key], |row| {
//                     Ok(Client::from(
//                         row.get(0).unwrap(),
//                         row.get(1).unwrap(),
//                         row.get(2).unwrap(),
//                         row.get(3).unwrap(),
//                         row.get(4).unwrap(),
//                         row.get(5).unwrap(),
//                         row.get(6).unwrap(),
//                         serde_json::from_str::<Vec<String>>(row.get::<_, String>(7)?.as_str()).unwrap(),
//                         row.get(8).unwrap(),
//                     ))
//                 })
//                 .unwrap();

//             for client in clients_iter {
//                 clients.push(client.unwrap());
//             }
//         }

//         if clients.len() == 0 {
//             return Err(ClientError::ClientDoesNotExist(client_key));
//         } else {
//             return Ok(clients[0]);
//         }
//     })
// }

// fn get_client_by_name(client_name: String) -> Result<Client, ClientError> {
//     with_connection!(SQL_POOL, |conn: &rusqlite::Connection| {
//         let mut clients: Vec<Client> = Vec::new();

//         {
//             let mut smtp = conn.prepare("SELECT * FROM Clients WHERE ClientName = ?").unwrap();

//             let clients_iter = smtp
//                 .query_map(params![client_name], |row| {
//                     Ok(Client::from(
//                         row.get(0).unwrap(),
//                         row.get(1).unwrap(),
//                         row.get(2).unwrap(),
//                         row.get(3).unwrap(),
//                         row.get(4).unwrap(),
//                         row.get(5).unwrap(),
//                         row.get(6).unwrap(),
//                         serde_json::from_str::<Vec<String>>(row.get::<_, String>(7)?.as_str()).unwrap(),
//                         row.get(8).unwrap(),
//                     ))
//                 })
//                 .unwrap();

//             for client in clients_iter {
//                 clients.push(client.unwrap());
//             }
//         }

//         if clients.len() == 0 {
//             return Err(ClientError::ClientDoesNotExist(client_name));
//         } else {
//             return Ok(clients[0]);
//         }
//     })
// }

// pub fn edit_client(client: Client) {
//     with_connection!(SQL_POOL, |conn: &rusqlite::Connection| {
//         let serialized_owned_sub_channels_keys = serde_json::to_string(&client.owned_sub_channels_keys).expect("Failed to serialize to JSON");

//         let result = conn.execute(
//             "UPDATE Clients SET ClientName = ?, ClientKey = ?, PermissionGroup = ?, SuperUser = ?, LastContact = ?, MaxSubChannels = ?, OwnedSubChannelsKeys = ?, SubChannelsInUse = ? WHERE ID = ?;",
//             params![
//                 client.client_name,
//                 client.client_key,
//                 client.permission_group,
//                 client.super_user,
//                 client.last_contact,
//                 client.max_sub_channels,
//                 serialized_owned_sub_channels_keys,
//                 client.sub_channels_in_use,
//                 client.client_id,
//             ],
//         );

//         match result {
//             Ok(rows) => {
//                 if rows > 0 {
//                     println!("Successfully update client: {} in database", client.client_name);
//                 }
//             },
//             Err(e) => {
//                 eprintln!("Error while update client: {} in the database, the error is: {}", client.client_name, e);
//             },
//         }
//     });
// }

// fn remove_client(client: Client) {
//     with_connection!(SQL_POOL, |conn: &rusqlite::Connection| {
//         let result = conn.execute("DELETE from Clients WHERE ClientKey = ?", params![client.client_key]);

//         match result {
//             Ok(rows) => {
//                 println!("Successfully deleted Client: {} from clients! {} Rows were affected.", client.client_key, rows);
//             },
//             Err(e) => {
//                 eprintln!("An error occurred while deleting Client: {} from clients! And the error was: {}", client.client_key, e);
//             },
//         }
//     });
// }
