use crate::{common::enhanced_buffer::utilities::CommandOrigin, CommandInstructions};
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
    origin: String,
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
    return ts.timestamp() as f64 + (ts.timestamp_subsec_nanos() as f64) / 1e9f64;
}

// 1e9    -> 1_000_000_000      cause 1x10^9    == 1_000_000_000
// 1e9f64 -> 1_000_000_000.0    cause 1.0x10^9  == 1_000_000_000.0

fn float_to_ts(f: f64) -> DateTime<Utc> {
    // Convert f64 back to DateTime<Utc>
    let seconds = f.trunc() as i64;
    let nanos = ((f.fract() * 1e9f64).round() as u32) % 1e9 as u32;
    let reconstructed_time = Utc.timestamp(seconds, nanos);
    return reconstructed_time;
}

impl NodeTask {
    pub fn new(origin: String, parity_id: String, command: CommandInstructions) -> Self {
        let received_ts: f64 = ts_to_float(Utc::now());
        Self {
            origin,
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

    pub fn trace_origin(self) -> CommandOrigin {
        self.command.origin
    }

    pub fn get_origin(self) -> String {
        self.origin
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
    pub fn new_empty() -> Self {
        Self { tasks: HashMap::new() }
    }

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

    pub fn get_node_task_origin(&mut self, node_key: &String, parity_id: &String) -> Result<String, TaskManagerError> {
        let mut tasks = self.get_node_tasks(node_key)?;
        let task = tasks.iter().find(|&task| &task.parity_id == parity_id);
        if let Some(task) = task {
            return Ok(task.clone().get_origin());
        }
        return Err(TaskManagerError::TaskNotFound(node_key.clone(), parity_id.clone()));
    }

    pub fn show_node_tasks(&mut self, node_key: &String) -> Result<(), TaskManagerError> {
        let node_tasks = self.get_node_tasks(node_key)?;
        println!("\n\n");
        println!("Node: {} tasks:", node_key);
        for task in node_tasks {
            println!("task: {:?}", task);
        }
        println!("\n\n");
        Ok(())
    }

    pub fn add_task_to_node(&mut self, node_key: &String, task: NodeTask) -> Result<(), TaskManagerError> {
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

    pub fn remove_task_from_node(&mut self, node_key: &String, parity_id: &String) -> Result<(), TaskManagerError> {
        let mut tasks = self.get_node_tasks(node_key)?;
        tasks.retain(|t| t.parity_id == *parity_id);
        Ok(())
    }
}

//* The idea:
//* - Use this system to registry a task to some node when receive a data to redirect, and registry
//*   a task to self host when received one task to self host.
//*
//* - When some node send back a response to redirect to origin, use the task ref to locate origin
//*   based in the command that generated this task that will be able to be finded using the client id
//*   and the parity id of this task.
//*
//* -
//*
//*
//*
//*
