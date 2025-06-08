use lazy_static::lazy_static;

#[macro_use]
use crate::{with_connection, set_new_path_to_buffer_db};
use crate::common::sql_pool::pool::{SQLiteConnectionPool, UniqueParityIdGenerator};

use rusqlite::params;

use serde::{Deserialize, Serialize};

use core::fmt;
use std::clone;
use std::io::BufReader;
use std::sync::Arc;
use tokio::sync::Mutex;

use serde_json::{from_str, Value};
use std::collections::HashMap;

use crate::common::enhanced_buffer::utilities::Command;
use crate::common::types::BufferError;
use chrono::Utc;
use rusqlite::{Connection, Result};
use std::sync::RwLock;

use crate::common::enhanced_buffer::history::buffer_history::BufferHistory;

lazy_static! {
    static ref BUFFER_NAME: Arc<Mutex<String>> = Arc::new(Mutex::new("buffer.db".to_string()));
    static ref BUFFER_PATH: Arc<Mutex<String>> = Arc::new(Mutex::new("buffer.db".to_string()));
    static ref NUM_WORKERS: Arc<Mutex<u32>> = Arc::new(Mutex::new(5));
    static ref BUFFER_POOL: Arc<Mutex<SQLiteConnectionPool>> = Arc::new(Mutex::new(SQLiteConnectionPool::empty()));
}

/*
   However, the rusqlite library in Rust automatically starts a new
   transaction before each command and commits it after the command
   is executed, unless you explicitly start a transaction. This is
   known as "autocommit mode".

*/

pub async fn set_workers_num(n_workers: u32) {
    {
        println!("[CLIENT][GLOBAL][Try Lock] - NUM_WORKERS");
        let mut default_num_of_workers = NUM_WORKERS.lock().await;
        println!("[CLIENT][GLOBAL][Lock] - NUM_WORKERS");
        *default_num_of_workers = n_workers;
    }
    println!("[CLIENT][GLOBAL][Release] - NUM_WORKERS");
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct UpCommand {
    pub command_id: Option<u32>,
    pub client_key: String,
    pub parity_id: String,
    pub priority: u8,
    pub command: String,
    pub created_time: f64,
}

impl UpCommand {
    pub fn from(command_id: u32, client_key: String, parity_id: String, priority: u8, command: String, created_time: f64) -> Self {
        let now = Utc::now();
        let timestamp = now.timestamp() as f64 + (now.timestamp_subsec_millis() as f64 / 1000.0);

        Self {
            command_id: Some(command_id),
            client_key,
            parity_id,
            priority,
            command,
            created_time,
        }
    }

    pub fn new(client_key: &String, parity_id: &String, priority: u8, command: &String) -> Self {
        let now = Utc::now();
        let timestamp = now.timestamp() as f64 + (now.timestamp_subsec_millis() as f64 / 1000.0);

        Self {
            command_id: Some(0000u32),
            client_key: client_key.clone(),
            parity_id: parity_id.clone(),
            priority,
            command: command.clone(),
            created_time: timestamp,
        }
    }

    pub fn from_command(command: Command) -> Self {
        let client_key = command.client_key;
        let parity_id = command.parity_id;
        let priority = command.priority;
        let command = serde_json::to_string(&command.command).unwrap();

        let now = Utc::now();
        let timestamp = now.timestamp() as f64 + (now.timestamp_subsec_millis() as f64 / 1000.0);

        Self {
            command_id: Some(0000u32),
            client_key,
            parity_id,
            priority,
            command,
            created_time: timestamp,
        }
    }
}

impl fmt::Display for UpCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "\nUpCommand: 
            command_id: {:?}\n
            client_key: {}\n
            parity_id: {}\n
            priority: {}\n
            command: {}\n\n",
            self.command_id, self.client_key, self.parity_id, self.priority, self.command
        )
    }
}

// impl IntoPy<PyObject> for UpCommand {
//     fn into_py(self, py: Python) -> PyObject {
//         let dict = PyDict::new(py);
//         dict.set_item("command_id", self.command_id).unwrap();
//         dict.set_item("client_key", self.client_key).unwrap();
//         dict.set_item("parity_id", self.parity_id).unwrap();
//         dict.set_item("priority", self.priority).unwrap();
//         dict.set_item("command", self.command).unwrap();
//         dict.into()
//     }
// }

fn get_registred_ids(conn: &Connection) -> Vec<u32> {
    let mut ids: Vec<u32> = Vec::new();

    {
        let mut smtp = conn.prepare("SELECT * FROM ClientCommandsTosend").unwrap();
        let commands_iter = smtp
            .query_map(params![], |row| {
                let id: u32 = row.get(0).unwrap();
                Ok(id)
            })
            .unwrap();

        for id in commands_iter {
            ids.push(id.unwrap());
        }
    }

    ids
}

use std::thread;
use std::time::Duration;

pub async fn buffer_up_initialize_table(buffer_path: String) {
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

    let fut = set_new_path_to_buffer_db!(BUFFER_POOL, NUM_WORKERS, buffer_path, BUFFER_NAME);
    fut.await;

    with_connection!(BUFFER_POOL, |conn: rusqlite::Connection| async {
        let sql = format!("DROP TABLE IF EXISTS ClientCommandsTosend");
        match conn.execute(&sql, params![]) {
            Ok(_) => {
                println!("Successfully dropped table ClientCommandsTosend");
            },
            Err(e) => {
                eprintln!("An error occurred while dropping the table ClientCommandsTosend: {}", e);
            },
        };

        let result = conn.execute(
            "CREATE TABLE IF NOT EXISTS ClientCommandsTosend (ID INTEGER PRIMARY KEY AUTOINCREMENT, ClientKey TEXT, ParityId TEXT, Priority NUMBER, Command TEXT, CreatedTime NUMBER)",
            params![],
        );

        match result {
            Ok(_) => {
                println!("Successfully initialize ClientCommandsTosend table!");
            },
            Err(e) => {
                eprintln!("An error occurred while scheduling the command in the ClientCommandsTosend table: {}", e);
            },
        };

        ((), conn)
    })
    .await;
}

async fn get_registered_parity_ids(client_key: String) -> Result<Vec<String>, BufferError> {
    with_connection!(BUFFER_POOL, |conn: rusqlite::Connection| async {
        let ids: Result<Vec<String>, BufferError> = 'loading: {
            let mut parity_ids: Vec<String> = Vec::new();

            let mut stmt = match conn.prepare("SELECT * FROM ClientCommandsTosend WHERE ClientKey = ? ") {
                Ok(smtp) => smtp,
                Err(e) => break 'loading Err(BufferError::UnexpectedError(format!("Loading commands failed: {e}"))),
            };

            let commands_iter = match stmt.query_map(params![client_key], |row| {
                let parity_id: String = row.get(2)?;
                Ok(parity_id)
            }) {
                Ok(cmi) => cmi,
                Err(e) => break 'loading Err(BufferError::UnexpectedError(format!("Commands iter during load preparation failed: {e}"))),
            };

            for command in commands_iter {
                match command {
                    Ok(c) => parity_ids.push(c),
                    Err(e) => break 'loading Err(BufferError::UnexpectedError(format!("Commands iter during load preparation failed: {e}"))),
                }
            }

            break 'loading Ok(parity_ids);
        };

        (ids, conn)
    })
    .await
}

pub async fn buffer_up_gen_valid_special_parity_id(client_key: &String) -> Result<String, BufferError> {
    let registred_ids: Vec<String> = get_registered_parity_ids(client_key.clone()).await?;
    let mut unique_parity_id_generator = UniqueParityIdGenerator::new(20, registred_ids);
    let valid_parity_id: String = unique_parity_id_generator.gen();
    return Ok(valid_parity_id);
}

pub async fn buffer_up_gen_valid_parity_id(client_key: String) -> Result<String, BufferError> {
    let registred_ids: Vec<String> = get_registered_parity_ids(client_key).await?;
    let mut unique_parity_id_generator = UniqueParityIdGenerator::new(16, registred_ids);
    let valid_parity_id: String = unique_parity_id_generator.gen();
    return Ok(valid_parity_id);
}

pub async fn buffer_up_get_scheduled_by_parity_id(client_key: &String, parity_id: &String) -> Result<Vec<UpCommand>, BufferError> {
    with_connection!(BUFFER_POOL, |conn: rusqlite::Connection| async {
        let commands_schedule = 'loading: {
            let mut commands_schedule: Vec<UpCommand> = Vec::new();
            let mut smtp = match conn.prepare("SELECT * FROM ClientCommandsTosend WHERE ClientKey = ? AND ParityId = ?") {
                Ok(smtp) => smtp,
                Err(e) => break 'loading Err(BufferError::UnexpectedError(format!("Loading commands failed: {e}"))),
            };

            let commands_iter = match smtp.query_map(params![client_key, parity_id], |row| {
                Ok(UpCommand::from(row.get(0).unwrap(), row.get(1).unwrap(), row.get(2).unwrap(), row.get(3).unwrap(), row.get(4).unwrap(), row.get(5).unwrap()))
            }) {
                Ok(cmi) => cmi,
                Err(e) => break 'loading Err(BufferError::UnexpectedError(format!("Commands iter during load preparation failed: {e}"))),
            };

            for command in commands_iter {
                match command {
                    Ok(c) => commands_schedule.push(c),
                    Err(e) => break 'loading Err(BufferError::UnexpectedError(format!("Commands iter during load preparation failed: {e}"))),
                }
            }

            break 'loading Ok(commands_schedule);
        };

        (commands_schedule, conn)
    })
    .await
}

pub async fn buffer_up_list_schedule_fo_client_id(client_key: String) -> Result<Vec<UpCommand>, BufferError> {
    with_connection!(BUFFER_POOL, |conn: rusqlite::Connection| async {
        let commands_schedule = 'loading: {
            let mut commands_schedule: Vec<UpCommand> = Vec::new();
            let mut smtp = match conn.prepare("SELECT * FROM ClientCommandsTosend WHERE ClientKey = ?") {
                Ok(smtp) => smtp,
                Err(e) => break 'loading Err(BufferError::UnexpectedError(format!("Loading commands failed: {e}"))),
            };

            let commands_iter = match smtp.query_map(params![client_key], |row| {
                Ok(UpCommand::from(row.get(0).unwrap(), row.get(1).unwrap(), row.get(2).unwrap(), row.get(3).unwrap(), row.get(4).unwrap(), row.get(5).unwrap()))
            }) {
                Ok(cmi) => cmi,
                Err(e) => break 'loading Err(BufferError::UnexpectedError(format!("Commands iter during load preparation failed: {e}"))),
            };

            for command in commands_iter {
                match command {
                    Ok(c) => commands_schedule.push(c),
                    Err(e) => break 'loading Err(BufferError::UnexpectedError(format!("Commands iter during load preparation failed: {e}"))),
                }
            }

            break 'loading Ok(commands_schedule);
        };

        (commands_schedule, conn)
    })
    .await
}

pub async fn buffer_up_list_schedule() -> Result<Vec<UpCommand>, BufferError> {
    with_connection!(BUFFER_POOL, |conn: rusqlite::Connection| async {
        let commands_schedule = 'loading: {
            let mut stmt = match conn.prepare("SELECT * FROM ClientCommandsTosend") {
                Ok(smtp) => smtp,
                Err(e) => break 'loading Err(BufferError::UnexpectedError(format!("Loading commands failed: {e}"))),
            };

            let commands_iter = match stmt.query_map([], |row| Ok(UpCommand::from(row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?))) {
                Ok(cmi) => cmi,
                Err(e) => break 'loading Err(BufferError::UnexpectedError(format!("Commands iter during load preparation failed: {e}"))),
            };

            let mut commands_schedule: Vec<UpCommand> = Vec::new();
            for command in commands_iter {
                match command {
                    Ok(c) => commands_schedule.push(c),
                    Err(e) => break 'loading Err(BufferError::UnexpectedError(format!("Commands iter during load preparation failed: {e}"))),
                }
            }

            break 'loading Ok(commands_schedule);
        };

        (commands_schedule, conn)
    })
    .await
}
pub async fn buffer_up_schedule(command: UpCommand) -> Result<(), BufferError> {
    BufferHistory::new("UP").log_add_operation(&command.client_key, &command.parity_id, command.command_id.as_ref(), &command.command).await;
    with_connection!(BUFFER_POOL, |conn: rusqlite::Connection| async {
        let result = 'loading: {
            let registered_ids = get_registred_ids(&conn);

            let now = Utc::now();
            let timestamp = now.timestamp() as f64 + (now.timestamp_subsec_millis() as f64 / 1000.0);

            let result = conn.execute(
                "INSERT INTO ClientCommandsTosend (ClientKey, ParityId, Priority, Command, CreatedTime) VALUES (?, ?, ?, ?, ?);",
                params![command.client_key, command.parity_id, command.priority, command.command, timestamp],
            );

            match result {
                Ok(_) => {
                    println!("Successfully schedule Command in ClientCommandsTosend");
                },
                Err(e) => break 'loading Err(BufferError::UnexpectedError(format!("An error occurred while scheduling the command in the ClientCommandsTosend table: {}", e))),
            };

            break 'loading Ok(());
        };

        (result, conn)
    })
    .await
}

pub async fn check_if_parity_id_is_registered(parity_id: String, client_key: String) -> Result<bool, BufferError> {
    with_connection!(BUFFER_POOL, |conn: rusqlite::Connection| async {
        let result: Result<bool, BufferError> = 'loading: {
            let mut smtp = match conn.prepare("SELECT * FROM ClientCommandsTosend WHERE ClientKey = ?") {
                Ok(smtp) => smtp,
                Err(e) => break 'loading Err(BufferError::UnexpectedError(format!("Loading commands failed: {e}"))),
            };

            let commands_iter = smtp
                .query_map(params![client_key], |row| {
                    let id: String = row.get(2).unwrap();
                    Ok(id)
                })
                .unwrap();

            for id in commands_iter {
                match id {
                    Ok(id) => {
                        if parity_id == id {
                            break 'loading Ok(false);
                        }
                    },
                    Err(e) => {
                        break 'loading Err(BufferError::UnexpectedError(format!("An error occurred while check if parity_id is registred in the ClientCommandsTosend table: {}", e)));
                    },
                }
            }

            break 'loading Ok(false);
        };

        (result, conn)
    })
    .await
}

pub async fn buffer_up_update_schedule(id: i32, client_key: String, parity_id: String, priority: i32, command: String) -> Result<(), BufferError> {
    with_connection!(BUFFER_POOL, |conn: rusqlite::Connection| async {
        let result = conn.execute(
            "Update ClientCommandsTosend set ClientKey = ?, ParityId = ?, Priority = ?, Command = ? where ID = ?",
            params![client_key, parity_id, priority, command, id],
        );

        let result = match result {
            Ok(_) => {
                println!("Successfully update Command in ClientCommandsTosend");
                Ok(())
            },
            Err(e) => Err(BufferError::UnexpectedError(format!("An error occurred while update the command in the ClientCommandsTosend table: {}", e))),
        };

        (result, conn)
    })
    .await
}

pub async fn buffer_up_clear_old_commands() -> Result<(), BufferError> {
    let now = Utc::now();
    let current_timestamp = now.timestamp() as f64 + (now.timestamp_subsec_millis() as f64 / 1000.0);
    let schedule = buffer_up_list_schedule().await?;

    if (schedule.is_empty()) {
        return Ok(());
    }

    for up_command in schedule {
        let command_timestamp = up_command.created_time;
        let time_difference = (current_timestamp - command_timestamp);
        if time_difference >= 240.0 {
            BufferHistory::new("UP")
                .log_remove_operation(&up_command.client_key, &up_command.parity_id, up_command.command_id.as_ref(), &up_command.command)
                .await;
            buffer_up_remove_schedule_by_id(up_command.command_id.unwrap()).await?;
            println!("\nCommand received from host: {} from client: {}, too old, clearing from the buffer up schedule!\n", up_command.parity_id, up_command.client_key);
        }
    }

    return Ok(());
}

pub async fn buffer_up_remove_schedule_by_id(id: u32) -> Result<(), BufferError> {
    BufferHistory::new("UP").log_remove_operation(&"".to_string(), &"".to_string(), Some(id).as_ref(), &format!("Remove ID: {}", id)).await;
    with_connection!(BUFFER_POOL, |conn: rusqlite::Connection| async {
        let result = conn.execute("DELETE from ClientCommandsTosend where ID = ?", params![id]);
        let result = match result {
            Ok(_) => {
                println!("Successfully removed scheduled Command of id: {} in ClientCommandsTosend", id);
                Ok(())
            },
            Err(e) => Err(BufferError::UnexpectedError(format!(
                "An error occurred while removing the scheduled the command of id: {} in the ClientCommandsTosend table: {}",
                id, e
            ))),
        };

        (result, conn)
    })
    .await
}

pub async fn buffer_up_remove_schedule_by_parity_id(client_key: &String, parity_id: &String) -> Result<(), BufferError> {
    BufferHistory::new("UP").log_remove_operation(&client_key, &parity_id, None.as_ref(), &"Remove From Schedule".to_string()).await;

    with_connection!(BUFFER_POOL, |conn: rusqlite::Connection| async {
        let result = conn.execute("DELETE from ClientCommandsTosend WHERE ClientKey = ? AND ParityId = ?", params![client_key, parity_id]);

        let result = match result {
            Ok(_) => {
                println!("Successfully remove schedule Command in ClientCommandsTosend where ClientKey = {} AND ParityID = {}", client_key, parity_id);
                Ok(())
            },
            Err(e) => Err(BufferError::UnexpectedError(format!(
                "An error occurred while removing scheduled command of parity_id: {} from client: {} in the ClientCommandsTosend table: {}",
                client_key, parity_id, e
            ))),
        };

        (result, conn)
    })
    .await
}
