use lazy_static::lazy_static;

#[macro_use]
use crate::{with_connection, set_new_path_to_buffer_db};
use crate::common::sql_pool::pool::{SQLiteConnectionPool, UniqueParityIdGenerator};

use rusqlite::params;

use serde::{Deserialize, Serialize};

use std::clone;
use std::sync::Arc;

use parking_lot::Mutex;
use std::thread;
use std::time::Duration;

use serde_json::{from_str, Value};
use std::collections::HashMap;

use chrono::Utc;

use crate::common::enhanced_buffer::utilities::Command;

use std::sync::RwLock;

use rusqlite::{Connection, Result};

use crate::common::enhanced_buffer::history::buffer_history::BufferHistory;

use std::fmt;

use super::utilities::CommandMode;

lazy_static! {
    static ref BUFFER_NAME: Arc<Mutex<String>> = Arc::new(Mutex::new("buffer.db".to_string()));
    static ref BUFFER_PATH: Arc<Mutex<String>> = Arc::new(Mutex::new("buffer.db".to_string()));
    static ref NUM_WORKERS: Arc<Mutex<u32>> = Arc::new(Mutex::new(5));
    static ref BUFFER_POOL: Mutex<SQLiteConnectionPool> = Mutex::new(SQLiteConnectionPool::empty());
}

// static ref BUFFER_POOL: SQLiteConnectionPool = {
//     let buffer_path_clone;
//     let num_workers_clone;
//     {
//         let buffer_path = BUFFER_PATH.lock().unwrap();
//         buffer_path_clone = buffer_path.clone();

//         let num_workers = NUM_WORKERS.lock().unwrap();
//         num_workers_clone = num_workers.clone() as usize
//     }
//     SQLiteConnectionPool::new(num_workers_clone, buffer_path_clone.as_str()).unwrap()
// };

/*
   However, the rusqlite library in Rust automatically starts a new
   transaction before each command and commits it after the command
   is executed, unless you explicitly start a transaction. This is
   known as "autocommit mode".

*/

pub fn set_workers_num(n_workers: u32) {
    {
        println!("[CLIENT][GLOBAL][Try Lock] - NUM_WORKERS");
        let mut default_num_of_workers = NUM_WORKERS.lock();
        println!("[CLIENT][GLOBAL][Lock] - NUM_WORKERS");
        *default_num_of_workers = n_workers;
    }
    println!("[CLIENT][GLOBAL][Release] - COMMAND_PATTERNS");
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DownCommand {
    pub command_id: Option<u32>,
    pub client_key: String,
    pub parity_id: String,
    pub priority: u8,
    pub command: String,
    pub command_mode: CommandMode,
    pub created_time: f64,
    pub auto_collect: bool,
}

impl fmt::Display for DownCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "\nUpCommand: 
            command_id: {:?}\n
            client_key: {}\n
            parity_id: {}\n
            priority: {}\n
            command: {}\n
            auto_collect: {}\n\n",
            self.command_id, self.client_key, self.parity_id, self.priority, self.command, self.auto_collect
        )
    }
}

impl DownCommand {
    pub fn from(command_id: u32, client_key: String, parity_id: String, priority: u8, command: String, command_type: String, created_time: f64, auto_collect: bool) -> Self {
        let now = Utc::now();
        let timestamp = now.timestamp() as f64 + (now.timestamp_subsec_millis() as f64 / 1000.0);

        let command_mode_downcast: CommandMode = serde_json::from_str(&command_type).unwrap();

        Self {
            command_id: Some(command_id),
            client_key,
            parity_id,
            priority,
            command,
            command_mode: command_mode_downcast,
            created_time,
            auto_collect,
        }
    }

    pub fn new(client_key: String, parity_id: String, priority: u8, command: String, command_mode: CommandMode, auto_collect: bool) -> Self {
        let now = Utc::now();
        let timestamp = now.timestamp() as f64 + (now.timestamp_subsec_millis() as f64 / 1000.0);

        Self {
            command_id: Some(0000u32),
            client_key,
            parity_id,
            priority,
            command,
            command_mode,
            created_time: timestamp,
            auto_collect,
        }
    }

    pub fn from_command(command: Command) -> Self {
        let client_key = command.client_key;
        let parity_id = command.parity_id;
        let priority = command.priority;
        let auto_collect = command.command.collect_response.clone();
        let command_mode = command.command.mode.clone();
        let command = serde_json::to_string(&command.command).unwrap();

        let now = Utc::now();
        let timestamp = now.timestamp() as f64 + (now.timestamp_subsec_millis() as f64 / 1000.0);

        Self {
            command_id: Some(0000u32),
            client_key,
            parity_id,
            priority,
            command,
            command_mode,
            created_time: timestamp,
            auto_collect,
        }
    }
}

// impl IntoPy<PyObject> for DownCommand {
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
        let mut smtp = conn.prepare("SELECT * FROM ClientCommandsReceived").unwrap();
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

    ids.clone()
}

pub fn buffer_down_initialize_table(buffer_path: String) {
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

    set_new_path_to_buffer_db!(BUFFER_POOL, NUM_WORKERS, buffer_path, BUFFER_NAME);

    with_connection!(BUFFER_POOL, |conn: &rusqlite::Connection| {
        let sql = format!("DROP TABLE IF EXISTS ClientCommandsReceived");
        match conn.execute(&sql, params![]) {
            Ok(_) => {
                println!("Successfully dropped table ClientCommandsReceived");
            },
            Err(e) => {
                eprintln!("An error occurred while dropping the table ClientCommandsReceived: {}", e);
            },
        };

        let result = conn.execute(
            "CREATE TABLE IF NOT EXISTS ClientCommandsReceived (ID INTEGER PRIMARY KEY AUTOINCREMENT, Clientkey TEXT, ParityId TEXT, Priority NUMBER, Command TEXT, CommandMode TEXT, CreatedTime NUMBER, CollectIt BOOL)",
            params![],
        );

        match result {
            Ok(_) => {
                println!("Successfully initialize ClientCommandsReceived table!");
            },
            Err(e) => {
                eprintln!("An error occurred while scheduling the command in the ClientCommandsReceived table: {}", e);
            },
        };
    });
}

fn get_registered_parity_ids(client_key: String) -> Vec<String> {
    with_connection!(BUFFER_POOL, |conn: &rusqlite::Connection| {
        let mut parity_ids: Vec<String> = Vec::new();

        let mut stmt = conn.prepare("SELECT * FROM ClientCommandsReceived WHERE Clientkey = ? ").unwrap();
        let commands_iter = stmt
            .query_map(params![client_key], |row| {
                let parity_id: String = row.get(2)?;
                Ok(parity_id)
            })
            .unwrap();

        for command in commands_iter {
            parity_ids.push(command.unwrap());
        }

        parity_ids
    })
}

pub fn buffer_down_gen_valid_parity_id(client_key: String) -> String {
    let registred_ids: Vec<String> = get_registered_parity_ids(client_key);

    let mut unique_parity_id_generator = UniqueParityIdGenerator::new(16, registred_ids);

    let valid_parity_id: String = unique_parity_id_generator.gen();

    return valid_parity_id;
}

pub fn buffer_down_get_scheduled_by_parity_id(client_key: String, parity_id: String) -> Vec<DownCommand> {
    with_connection!(BUFFER_POOL, |conn: &rusqlite::Connection| {
        let mut commands_schedule: Vec<DownCommand> = Vec::new();

        {
            let mut smtp = conn.prepare("SELECT * FROM ClientCommandsReceived WHERE Clientkey = ? AND ParityId = ?").unwrap();

            let commands_iter = smtp
                .query_map(params![client_key, parity_id], |row| {
                    Ok(DownCommand::from(
                        row.get(0).unwrap(),
                        row.get(1).unwrap(),
                        row.get(2).unwrap(),
                        row.get(3).unwrap(),
                        row.get(4).unwrap(),
                        row.get(5).unwrap(),
                        row.get(6).unwrap(),
                        row.get(7).unwrap(),
                    ))
                })
                .unwrap();

            for command in commands_iter {
                commands_schedule.push(command.unwrap());
            }
        }

        commands_schedule
    })
}

pub fn buffer_down_list_schedule() -> Vec<DownCommand> {
    with_connection!(BUFFER_POOL, |conn: &rusqlite::Connection| {
        let mut commands_schedule: Vec<DownCommand> = Vec::new();
        {
            let mut smtp = conn.prepare("SELECT * FROM ClientCommandsReceived").unwrap();

            let commands_iter = smtp
                .query_map(params![], |row| {
                    Ok(DownCommand::from(
                        row.get(0).unwrap(),
                        row.get(1).unwrap(),
                        row.get(2).unwrap(),
                        row.get(3).unwrap(),
                        row.get(4).unwrap(),
                        row.get(5).unwrap(),
                        row.get(6).unwrap(),
                        row.get(7).unwrap(),
                    ))
                })
                .unwrap();

            for command in commands_iter {
                commands_schedule.push(command.unwrap());
            }
        }
        commands_schedule
    })
}

pub fn buffer_down_schedule(command: &DownCommand) {
    if !check_if_parity_id_is_registred(&command.parity_id) {
        return;
    };

    BufferHistory::new("DOWN").log_add_operation(&command.client_key, &command.parity_id, command.command_id.as_ref(), &command.command);

    with_connection!(BUFFER_POOL, |conn: &rusqlite::Connection| {
        let registered_ids = get_registred_ids(conn);

        let now = Utc::now();
        let timestamp = now.timestamp() as f64 + (now.timestamp_subsec_millis() as f64 / 1000.0);

        let command_mode: String = serde_json::to_string(&command.command_mode).unwrap();

        let result = conn.execute(
            "INSERT INTO ClientCommandsReceived (Clientkey, ParityId, Priority, Command, CommandMode, CreatedTime, CollectIt) VALUES (?, ?, ?, ?, ?, ?, ?);",
            params![command.client_key, command.parity_id, command.priority, command.command, command_mode, timestamp, command.auto_collect],
        );

        match result {
            Ok(_) => {
                println!("Successfully schedule Command in ClientCommandsReceived");
            },
            Err(e) => {
                eprintln!("An error occurred while scheduling the command in the ClientCommandsReceived table: {}", e);
            },
        }
    });
}

pub fn check_if_parity_id_is_registred(parity_id: &String) -> bool {
    with_connection!(BUFFER_POOL, |conn: &rusqlite::Connection| {
        let mut ids: Vec<Result<String, _>> = Vec::new();

        {
            let mut smtp = conn.prepare("SELECT * FROM ClientCommandsReceived").unwrap();
            let commands_iter = smtp
                .query_map(params![], |row| {
                    let id: String = row.get(2).unwrap();
                    Ok(id)
                })
                .unwrap();

            for id in commands_iter {
                ids.push(id);
            }
        }

        for id in ids {
            match id {
                Ok(id) => {
                    if parity_id == &id {
                        return false;
                    }
                },
                Err(e) => {
                    eprintln!("An error occurred while check if parity_id is registred in the ClientCommandsReceived table: {}", e);
                },
            }
        }

        return true;
    })
}

pub fn buffer_down_update_schedule(id: i32, client_key: String, parity_id: String, priority: i32, command: String, command_type: String, auto_collect: bool) {
    with_connection!(BUFFER_POOL, |conn: &rusqlite::Connection| {
        let result = conn.execute(
            "UPDATE ClientCommandsReceived SET Clientkey = ?, ParityId = ?, Priority = ?, Command = ?, CommandMode = ?, CollectIt = ? WHERE ID = ?",
            params![client_key, parity_id, priority, command, command, command_type, auto_collect, id],
        );

        match result {
            Ok(_) => {
                println!("Successfully update Command in ClientCommandsReceived");
            },
            Err(e) => {
                eprintln!("An error occurred while update the command in the ClientCommandsReceived table: {}", e);
            },
        };
    });
}

pub fn buffer_down_clear_old_commands() {
    let now = Utc::now();
    let current_timestamp = now.timestamp() as f64 + (now.timestamp_subsec_millis() as f64 / 1000.0);

    let schedule = buffer_down_list_schedule();

    if (schedule.is_empty()) {
        return;
    }

    for down_command in schedule {
        let command_timestamp = down_command.created_time;

        let time_difference = (current_timestamp - command_timestamp);

        if time_difference >= 240.0 {
            BufferHistory::new("DOWN").log_remove_operation(&down_command.client_key, &down_command.parity_id, down_command.command_id.as_ref(), &format!("Remove old command: {} ", &down_command.command));

            buffer_down_remove_schedule_by_id(down_command.command_id.unwrap());
            println!(
                "\nCommand received from host: {} from client: {}, too old, clearing from the buffer down schedule!\n",
                down_command.parity_id, down_command.client_key
            );
        }
    }
}

pub fn buffer_down_remove_schedule_by_id(id: u32) {
    BufferHistory::new("DOWN").log_remove_operation(&"".to_string(), &"".to_string(), Some(id).as_ref(), &format!("Remove ID: {}", id));

    with_connection!(BUFFER_POOL, |conn: &rusqlite::Connection| {
        let result = conn.execute("DELETE FROM ClientCommandsReceived WHERE ID = ?", params![id]);

        match result {
            Ok(_) => {
                println!("Successfully removed scheduled Command of id: {} in ClientCommandsReceived", id);
            },
            Err(e) => {
                eprintln!("An error occurred while removing the scheduled the command of id: {} in the ClientCommandsReceived table: {}", id, e);
            },
        };
    });
}

pub fn buffer_down_remove_schedule_by_parity_id(client_key: String, parity_id: String) {
    BufferHistory::new("DOWN").log_remove_operation(&client_key, &parity_id, None.as_ref(), &"Remove From Schedule".to_string());

    with_connection!(BUFFER_POOL, |conn: &rusqlite::Connection| {
        let result = conn.execute("DELETE from ClientCommandsReceived where Clientkey = ? AND ParityId = ?", params![client_key, parity_id]);

        match result {
            Ok(_) => {
                println!("Successfully remove schedule Command in ClientCommandsReceived");
            },
            Err(e) => {
                eprintln!(
                    "An error occurred while removing scheduled command of parity_id: {} from client: {} in the ClientCommandsReceived table: {}",
                    client_key, parity_id, e
                );
            },
        }
    });
}
