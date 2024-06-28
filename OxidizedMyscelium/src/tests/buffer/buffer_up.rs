use crate::common::enhanced_buffer;
use crate::common::enhanced_buffer::buffer_up_manager::UpCommand;
use crate::common::enhanced_buffer::utilities::{Command, CommandInstructions, CommandMode, CommandOrigin, CommandStatus, CommandTarget, CommandType};
use serde_json::to_string;
use std::collections::HashMap;

#[test]
fn test_buffer_up_insertion() {
    // TODO >>> Make it test the buffer up

    //> The idea:

    // Write in the buffer
    // Read The buffer
    // Compare informations

    // Update row
    // Compare Informations
    // Delete

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
    let up_command = UpCommand::from_command(command);

    let command_to_schedule = UpCommand::new(&client_key.clone(), &parity_id, priority, &to_string(&up_command).unwrap());

    enhanced_buffer::buffer_up_manager::buffer_up_schedule(command_to_schedule);
}

#[test]
fn test_buffer_up_update() {
    // Update row
    // Compare Informations
}

#[test]
fn test_buffer_up_delete() {
    // Delete row create previously
    // Get Data
    // Verify delte
}

#[test]
fn test_buffer_up_capacity() {
    // TODO >>> Test large storage with multiple itens and compare results
}
