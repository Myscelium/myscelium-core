use crate::common::enhanced_buffer::buffer_up_manager::UpCommand;
use crate::common::enhanced_buffer::utilities::{Command, CommandInstructions, CommandMode, CommandOrigin, CommandStatus, CommandTarget, CommandType};
use crate::common::enhanced_buffer::{self, buffer_down_manager::DownCommand};
use serde_json::to_string;
use std::collections::HashMap;

#[test]
fn test_buffer_down() {
    //> The idea:

    // Write in the buffer
    // Read The buffer
    // Compare informations

    // Update row
    // Compare Informations
    // Delete

    // -> TEST ADD AND RETRIEVE

    enhanced_buffer::buffer_down_manager::buffer_down_initialize_table("./Temp/".to_string());

    let client_key: String = "randomsclientids".to_string();
    let parity_id = enhanced_buffer::buffer_up_manager::buffer_up_gen_valid_parity_id(client_key.clone());
    let priority = 1u8;

    let command_instruction = CommandInstructions::new(
        CommandMode::Function,
        CommandType::SpecialFunction,
        CommandTarget::Origin,
        CommandStatus::Success,
        CommandOrigin::Host,
        "test_function".to_string(),
        HashMap::new(),
        "".to_string(),
        Some(enhanced_buffer::utilities::ResponseType::SpecialFunction),
        Some(enhanced_buffer::utilities::ResponseTarget::Origin),
        Some("test_rsponse_handler".to_string()),
        true,
    );

    let command = Command::new(client_key.clone(), parity_id.clone(), priority, command_instruction);
    let down_command = DownCommand::from_command(command.clone());

    // Schedule command:

    enhanced_buffer::buffer_down_manager::buffer_down_schedule(&down_command);

    let buffer_list = enhanced_buffer::buffer_down_manager::buffer_down_list_schedule();
    let command_extracted = buffer_list.first().unwrap();
    let cm = Command::from_down_command(command_extracted).unwrap();

    assert_eq!(serde_json::to_string(&command).unwrap(), serde_json::to_string(&cm).unwrap());

    // -> TEST DELETE:

    let buffer_list = enhanced_buffer::buffer_down_manager::buffer_down_list_schedule();
    let command_extracted = buffer_list.first().unwrap();
    enhanced_buffer::buffer_down_manager::buffer_down_remove_schedule_by_id(command_extracted.command_id.unwrap());

    let buffer_list = enhanced_buffer::buffer_down_manager::buffer_down_list_schedule();
    assert_eq!(0, buffer_list.len());
}
