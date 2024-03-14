use lazy_static::lazy_static;

use serde_json::{from_str, Value};
use std::collections::HashMap;
use std::sync::{mpsc, Arc, Mutex};

use std::time::{SystemTime, UNIX_EPOCH};

use std::sync::atomic::AtomicBool;

use crate::HOST_LOG_LEVEL;
use crate::HOST_NODE_NAME;

use crate::socket_host::host_logger::register::register::write_to_file;
use serde_json::json;

// TODO >>> REMOVE CALLBACK SET AND CALLBACKS SYSTEM FOR LOG FOR NOW BECAUSE WE WILL USE CUSTOM REGISTER

lazy_static! {
    static ref CALLBACK_SET: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));
}

// TODO >>> Add a mechanism to set the node host name, to be able to identify in the logs

pub fn set_client_log_level(log_level: String) {
    {
        let mut current_log_level = HOST_LOG_LEVEL.lock();
        *current_log_level = log_level.clone();
        println!("Host log levels defined to: {:?}", log_level);
    }
}

// pub fn initialize_host_logs_database_dir(path: String) {
//     old_register_manager::logs_register_initialize_table(path);
// }

// pub fn set_host_logs_handler_callback(callback_pattern: HashMap<String, (Py<PyFunction>, Value)>) {
//     {
//         let mut heart_beat_callback = LOGS_HANDLER_CALLBACK.lock().unwrap();
//         *heart_beat_callback = callback_pattern;
//     }
//     CALLBACK_SET.store(true, Ordering::Relaxed);
// }

// This function takes log parameters and writes them to a file in a structured JSON format.
fn log_event(node_name: String, log_time: f64, log_name: String, log_level: String, log_msg: String) {
    // Serialize the log event into a JSON string.
    let log_entry = json!({
        "node_name": node_name,
        "log_time": log_time,
        "log_name": log_name,
        "log_level": log_level,
        "log_msg": log_msg
    })
    .to_string();

    // Write the JSON string to the log file.
    write_to_file(log_entry);
}

pub struct Logger {
    log_level: String,
    section: String,
    node_name: String,
}

impl Logger {
    pub fn new(log_level: String, section: &str) -> Self {
        // Placeholder for other initializations

        let node_name: String = HOST_NODE_NAME.lock().clone();

        Logger {
            log_level: log_level.to_string(),
            section: section.to_string(),
            node_name,
        }
    }

    pub fn debug(&self, log: String) {
        if self.log_level == "DEBUG" {
            let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs_f64();
            log_event(self.node_name.clone(), ts, self.section.clone(), "DEBUG".to_string(), log.to_string());
        }
    }

    pub fn info(&self, log: String) {
        if (self.log_level == "INFO") || (self.log_level == "DEBUG") {
            let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs_f64();
            log_event(self.node_name.clone(), ts, self.section.clone(), "INFO".to_string(), log.to_string());
        }
    }

    pub fn warn(&self, log: String) {
        if (self.log_level == "INFO") || (self.log_level == "WARN") || (self.log_level == "DEBUG") {
            let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs_f64();
            log_event(self.node_name.clone(), ts, self.section.clone(), "WARN".to_string(), log.to_string());
        }
    }

    pub fn exception(&self, log: String) {
        if (self.log_level == "INFO") || (self.log_level == "WARN") || (self.log_level == "DEBUG") || (self.log_level == "EXCEPTION") {
            let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs_f64();
            log_event(self.node_name.clone(), ts, self.section.clone(), "EXCEPTION".to_string(), log.to_string());
        }
    }
}
