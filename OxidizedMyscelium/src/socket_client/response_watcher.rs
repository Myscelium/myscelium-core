use std::thread;
use std::time::Duration;

use chrono::{DateTime, Utc};
use syn::token::Do;

use super::client_logger::log_handler::Logger;
use crate::common::enhanced_buffer;
use crate::common::enhanced_buffer::buffer_down_manager::DownCommand;
use crate::common::enhanced_buffer::utilities::Command;
use crate::CLIENT_LOG_LEVEL;

macro_rules! acquire_logger {
    ($section_name:expr) => {{
        let client_log_level;
        {
            let log_level = CLIENT_LOG_LEVEL.lock().clone();
            client_log_level = log_level.clone();
        }
        Logger::new(client_log_level, $section_name)
    }};
}

pub enum WatcherError {
    MaxTimeExceeded(String),
    CommandNotFinded(String),
}

pub fn watch_response(parity_id: String, max_time: chrono::Duration) -> Result<Command, WatcherError> {
    let logger = acquire_logger!("Transposer");

    let mut finded = false;
    let mut response_command: Option<DownCommand> = None;

    let start_time = Utc::now();

    loop {
        // Retrieve scheduled commands
        let mut schedule: Vec<DownCommand> = enhanced_buffer::buffer_down_manager::buffer_down_list_schedule();

        // -> Sort commands by priority in ascending order
        schedule.sort_by(|a, b| b.priority.cmp(&a.priority));

        // -> Filter only auto collect == false (that are commands to not autocollect)
        schedule = schedule.into_iter().filter(|s| !s.auto_collect).collect();

        for command in schedule {
            if command.parity_id == parity_id {
                finded = true;
                response_command = Some(command);
                break;
            }
        }

        let current_time = Utc::now();

        if current_time > (start_time + max_time) {
            return Err(WatcherError::MaxTimeExceeded(parity_id));
        }

        if finded {
            break;
        }

        thread::sleep(Duration::from_millis(50));
    }

    // TODO >>> Make the down command downcast erro handling for this case

    if let Some(response) = response_command {
        return Ok(Command::from_down_command(&response).unwrap());
    }

    return Err(WatcherError::CommandNotFinded(parity_id));
}
