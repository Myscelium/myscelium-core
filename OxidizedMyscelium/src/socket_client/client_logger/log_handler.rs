use lazy_static::lazy_static;

use serde_json::{from_str, Value};
use std::collections::HashMap;
use std::sync::{mpsc, Arc};

use std::time::{SystemTime, UNIX_EPOCH};

use std::sync::atomic::AtomicBool;

use crate::CLIENT_LOG_LEVEL;
use crate::CLIENT_NODE_NAME;

use crate::common::logs_register::register::write_to_file;
use serde_json::json;

// TODO >>> REMOVE CALLBACK SET AND CALLBACKS SYSTEM FOR LOG FOR NOW BECAUSE WE WILL USE CUSTOM REGISTER

lazy_static! {
    static ref CALLBACK_SET: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));
}

// TODO >>> Add a mechanism to set the node host name, to be able to identify in the logs

pub async fn set_client_log_level(log_level: String) {
    {
        let mut current_log_level = CLIENT_LOG_LEVEL.lock().await;
        *current_log_level = log_level.clone();
        println!("Client log levels defined to: {:?}", log_level);
    }
}

// pub fn set_host_logs_handler_callback(callback_pattern: HashMap<String, (Py<PyFunction>, Value)>) {
//     {
//         let mut heart_beat_callback = LOGS_HANDLER_CALLBACK.lock().unwrap();
//         *heart_beat_callback = callback_pattern;
//     }
//     CALLBACK_SET.store(true, Ordering::Relaxed);
// }

// This function takes log parameters and writes them to a file in a structured JSON format.
async fn log_event(node_name: String, log_time: f64, log_name: String, log_level: String, log_msg: String) {
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
    write_to_file(log_entry).await.unwrap();
}

pub struct Logger {
    log_level: String,
    section: String,
    node_name: String,
}

impl Logger {
    pub async fn new(log_level: String, section: &str) -> Self {
        // Placeholder for other initializations

        let node_name: String = CLIENT_NODE_NAME.lock().await.clone();

        Logger {
            log_level: log_level.to_string(),
            section: section.to_string(),
            node_name,
        }
    }

    pub async fn debug(&self, log: String) {
        if self.log_level == "DEBUG" {
            let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs_f64();
            log_event(self.node_name.clone(), ts, self.section.clone(), "DEBUG".to_string(), log.to_string()).await;
        }
    }

    pub async fn info(&self, log: String) {
        if (self.log_level == "INFO") || (self.log_level == "DEBUG") {
            let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs_f64();
            log_event(self.node_name.clone(), ts, self.section.clone(), "INFO".to_string(), log.to_string()).await;
        }
    }

    pub async fn warn(&self, log: String) {
        if (self.log_level == "INFO") || (self.log_level == "WARN") || (self.log_level == "DEBUG") {
            let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs_f64();
            log_event(self.node_name.clone(), ts, self.section.clone(), "WARN".to_string(), log.to_string()).await;
        }
    }

    pub async fn exception(&self, log: String) {
        if (self.log_level == "INFO") || (self.log_level == "WARN") || (self.log_level == "DEBUG") || (self.log_level == "EXCEPTION") {
            let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs_f64();
            log_event(self.node_name.clone(), ts, self.section.clone(), "EXCEPTION".to_string(), log.to_string()).await;
        }
    }
}
