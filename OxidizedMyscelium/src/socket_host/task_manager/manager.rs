use crate::CommandInstructions;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum TaskManagerError {
    #[error("Node key: {0} not found in the tasks map")]
    NodeNotFound(String),
    #[error("Node with key: {0} already exists!")]
    NodeAlreadyExists(String),
    #[error("Node with key: {0} don't have a task with parity_id: {1}!")]
    TaskNotFound(String, String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TaskStatus {
    WaitingReceiveConf,
    WaitingResponse,
    ScheduledToSend,
}

// If the command is scheduled to be sended to the target we
// use the ScheduledToSend, if we are waiting the rsponse we
// use the WaitingRespons end if the target doesn't confirm
// if it received the command or not after we send it then the
// status will become WaiingReceiveConf.

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeTask {
    parity_id: String,
    command: CommandInstructions,
    received_ts: f64,
    sended_to_target_ts: f64,
    received_by_target_ts: f64,
    status: TaskStatus,
}

extern crate chrono;
use chrono::{DateTime, TimeZone, Utc};

fn ts_to_float(ts: DateTime<Utc>) -> f64 {
    return ts.timestamp() as f64 + (ts.timestamp_subsec_nanos() as f64) / 1_000_000_000.0;
}

fn float_to_ts(f: f64) -> DateTime<Utc> {
    // Convert f64 back to DateTime<Utc>
    let seconds = f.trunc() as i64;
    let nanos = ((f.fract() * 1_000_000_000.0).round() as u32) % 1_000_000_000;
    let reconstructed_time = Utc.timestamp(seconds, nanos);
    return reconstructed_time;
}

impl NodeTask {
    pub fn new(parity_id: String, command: CommandInstructions) -> Self {
        let received_ts: f64 = ts_to_float(Utc::now());
        Self {
            parity_id,
            command,
            received_ts,
            sended_to_target_ts: -1f64,
            received_by_target_ts: -1f64,
            status: TaskStatus::ScheduledToSend,
        }
    }

    // Mark task as sended to the target
    pub fn sended(&mut self) {
        self.sended_to_target_ts = ts_to_float(Utc::now());
        self.status = TaskStatus::WaitingReceiveConf;
    }

    // Mark task as received by the target, current state waiting a response
    pub fn received_conf(&mut self) {
        self.received_by_target_ts = ts_to_float(Utc::now());
        self.status = TaskStatus::WaitingResponse;
    }
}

pub struct NodesTaskManager {
    tasks: HashMap<String, Vec<NodeTask>>,
}

//Manage Nodes
impl NodesTaskManager {
    pub fn add_node(&mut self, new_node_key: String) -> Result<(), TaskManagerError> {
        if self.tasks.contains_key(&new_node_key) {
            return Err(TaskManagerError::NodeAlreadyExists(new_node_key));
        };
        self.tasks.insert(new_node_key, Vec::new());
        return Ok(());
    }

    pub fn remove_node(&mut self, node_key: &String) {
        self.tasks.retain(|k, _| k == node_key);
    }
}

//Manage Node Tasks
impl NodesTaskManager {
    pub fn get_node_tasks(&mut self, node_key: &String) -> Result<&mut Vec<NodeTask>, TaskManagerError> {
        if let Some(tasks) = self.tasks.get_mut(node_key) {
            return Ok(tasks);
        } else {
            return Err(TaskManagerError::NodeNotFound(node_key.clone()));
        }
    }

    pub fn add_node_task(&mut self, node_key: &String, task: NodeTask) -> Result<(), TaskManagerError> {
        let mut node_tasks = self.get_node_tasks(node_key)?;
        node_tasks.push(task);
        Ok(())
    }

    pub fn get_node_task_by_id(&mut self, node_key: &String, parity_id: &String) -> Result<&mut NodeTask, TaskManagerError> {
        let mut node_tasks = self.get_node_tasks(node_key)?;
        for task in node_tasks {
            if task.parity_id == *parity_id {
                return Ok(task);
            }
            continue;
        }
        return Err(TaskManagerError::TaskNotFound(node_key.clone(), parity_id.clone()));
    }

    pub fn remove_task_of_a_node(&mut self, node_key: &String, parity_id: &String) -> Result<&mut NodeTask, TaskManagerError> {
        let mut tasks = self.get_node_tasks(node_key)?;
        tasks.retain(|&t| t.parity_id == *parity_id);
    }
}
