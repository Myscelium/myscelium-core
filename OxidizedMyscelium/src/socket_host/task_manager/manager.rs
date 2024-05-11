use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::CommandInstructions;

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

impl NodeTask {
    pub fn new() -> Self {}
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
}
