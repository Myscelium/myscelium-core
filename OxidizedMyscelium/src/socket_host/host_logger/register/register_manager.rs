// use std::hash::Hash;
// use std::sync::Mutex;

use std::sync::{Arc, Once};

use parking_lot::Mutex;
use std::thread;
use std::time::Duration;

use lazy_static::lazy_static;

// use std::collections::HashMap;

#[macro_use]
use crate::{with_connection, set_new_path_to_buffer_db};
use crate::common::sql_pool::pool::{
    SQLiteConnectionPool, UniqueIdGenerator, UniqueParityIdGenerator,
};

use rusqlite::params;
use serde::{Deserialize, Serialize};

// mod buffer_functions;

// use buffer_functions::UniqueIdGenerator;
// use buffer_functions::SQLiteConnectionPool;

// use pyo3::wrap_pyfunction;
// use pyo3::types::IntoPyDict;

// TODO >>> Add a mechanism toa automatically save the HostLogs into a  database and the client
//*         Add a system to store HostLogs from host
//*         Add a system to store clients last contact

//>     	Then make a interface in the python side to retrieve the HostLogs from the database
//>         And a system to retrieve the client last contact from the database

// -> DONE
lazy_static! {
    static ref BUFFER_NAME: Arc<Mutex<String>> = Arc::new(Mutex::new("Logs.db".to_string()));
    static ref BUFFER_PATH: Arc<Mutex<String>> = Arc::new(Mutex::new("Logs.db".to_string()));
    static ref NUM_WORKERS: Arc<Mutex<u32>> = Arc::new(Mutex::new(15));
    static ref LOGS_REGISTERS_POOL: Mutex<SQLiteConnectionPool> =
        Mutex::new(SQLiteConnectionPool::empty());
}

// -> DONE
pub fn set_workers_num(n_workers: u32) {
    let mut default_num_of_workers = NUM_WORKERS.lock();

    *default_num_of_workers = n_workers;
}

/*
   However, the rusqlite library in Rust automatically starts a new
   transaction before each command and commits it after the command
   is executed, unless you explicitly start a transaction. This is
   known as "auto-commit mode".

*/

#[derive(Serialize, Deserialize, Debug, Clone)] // -> DONE
pub struct Log {
    pub log_id: u32,
    pub node_name: String,
    pub log_time: f64,
    pub log_name: String,
    pub log_level: String,
    pub log_msg: String,
}

// -> DONE
fn get_registered_ids() -> Vec<u32> {
    with_connection!(LOGS_REGISTERS_POOL, |conn: &rusqlite::Connection| {
        let mut ids: Vec<u32> = Vec::new();

        {
            let mut smtp = conn.prepare("SELECT * FROM HostLogs").unwrap();
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

// -> DONE
pub fn logs_register_initialize_table(logs_storage_path: String) {
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

    set_new_path_to_buffer_db!(
        LOGS_REGISTERS_POOL,
        NUM_WORKERS,
        logs_storage_path,
        BUFFER_NAME
    );

    with_connection!(LOGS_REGISTERS_POOL, |conn: &rusqlite::Connection| {
        let result = conn.execute("CREATE TABLE IF NOT EXISTS HostLogs (ID INT PRIMARY KEY, NodeName TEXT, LogTime NUMBER, LogName TEXT, LogLevel TEXT, LogMsg TEXT)", params![]);

        match result {
            Ok(_) => {
                println!("Successfully initialize HostLogs table!");
            }
            Err(e) => {
                eprintln!(
                    "An error occurred while scheduling the command in the HostLogs table: {}",
                    e
                );
            }
        };
    })
}

// -> DONE
pub fn registry_log(
    node_name: String,
    log_time: f64,
    log_name: String,
    log_level: String,
    log_msg: String,
) {
    with_connection!(LOGS_REGISTERS_POOL, |conn: &rusqlite::Connection| {
        // let now = Utc::now();
        // let timestamp = now.timestamp() as f64 + (now.timestamp_subsec_millis() as f64 / 1000.0);

        let registered_ids = get_registered_ids();

        let mut id_generator = UniqueIdGenerator {
            registered_ids: registered_ids,
        };

        let result = conn.execute(
            "INSERT INTO HostLogs (ID, NodeName, LogTime, LogName, LogLevel, LogMsg) VALUES (?, ?, ?, ?, ?, ?);",
            params![id_generator.gen(), node_name, log_time, log_name, log_level, log_msg],
        );

        match result {
            Ok(rows) => {
                if rows > 0 {
                    // println!("Successfully inserted Log in the table HostLogs. {} row(s) were affected.", rows);
                } else {
                    // println!("No rows were affected.");
                }
            }
            Err(e) => {
                eprintln!(
                    "An error occurred while inserting the Log in the table HostLogs: {}",
                    e
                );
            }
        };
    })
}

// -> DONE
pub fn list_logs() -> Vec<Log> {
    with_connection!(LOGS_REGISTERS_POOL, |conn: &rusqlite::Connection| {
        let mut registred_logs: Vec<Log> = Vec::new();

        {
            let mut smtp = conn.prepare("SELECT * FROM HostLogs").unwrap();

            let logs_iter = smtp
                .query_map(params![], |row| {
                    Ok(Log {
                        log_id: row.get(0).unwrap(),
                        node_name: row.get(1).unwrap(),
                        log_time: row.get(2).unwrap(),
                        log_name: row.get(3).unwrap(),
                        log_level: row.get(4).unwrap(),
                        log_msg: row.get(5).unwrap(),
                    })
                })
                .unwrap();

            for log in logs_iter {
                match log {
                    Ok(l) => {
                        registred_logs.push(l);
                    }

                    Err(e) => {
                        println!("An error occurred while getting the HostLogs vec in list_logs, the error was: {}", e);
                    }
                }
            }
        }
        registred_logs
    })
}

pub fn remove_log_by_id(log_id: u32) {
    with_connection!(LOGS_REGISTERS_POOL, |conn: &rusqlite::Connection| {
        let result = conn.execute("DELETE from HostLogs where ID = ?", params![log_id]);

        match result {
            Ok(rows) => {
                println!(
                    "Successfully deleted Log of ID: {}. {} rows were affected.",
                    log_id, rows
                );
            }
            Err(e) => {
                eprintln!(
                    "An error occurred while deleting the Log: {} from HostLogs table: {}",
                    log_id, e
                );
            }
        };
    })
}
