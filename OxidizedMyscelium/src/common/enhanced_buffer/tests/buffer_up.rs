use crate::common::enhanced_buffer;
use crate::common::enhanced_buffer::buffer_up_manager::UpCommand;
use crate::common::enhanced_buffer::utilities::{Command, CommandInstructions, CommandMode, CommandOrigin, CommandStatus, CommandTarget, CommandType};
use serde_json::to_string;
use std::collections::HashMap;

// This test will test basic functionalities:
#[test]
fn test_buffer_up() {
    //> The idea:

    // Write in the buffer
    // Read The buffer
    // Compare informations

    // Update row
    // Compare Informations
    // Delete

    // -> TEST ADD AND RETRIEVE

    enhanced_buffer::buffer_up_manager::buffer_up_initialize_table("./Temp/".to_string());

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
    let up_command = UpCommand::from_command(command.clone());

    // Schedule command:

    enhanced_buffer::buffer_up_manager::buffer_up_schedule(up_command);

    let buffer_list = match enhanced_buffer::buffer_up_manager::buffer_up_list_schedule() {
        Ok(bup) => bup,
        Err(e) => panic!("{:?}", e),
    };

    let command_extracted = buffer_list.first().unwrap();
    let cm = Command::from_up_command(command_extracted).unwrap();

    assert_eq!(serde_json::to_string(&command).unwrap(), serde_json::to_string(&cm).unwrap());

    // -> TEST DELETE:

    let buffer_list = match enhanced_buffer::buffer_up_manager::buffer_up_list_schedule() {
        Ok(bup) => bup,
        Err(e) => panic!("{:?}", e),
    };

    let command_extracted = buffer_list.first().unwrap();
    enhanced_buffer::buffer_up_manager::buffer_up_remove_schedule_by_id(command_extracted.command_id.unwrap());

    let buffer_list = match enhanced_buffer::buffer_up_manager::buffer_up_list_schedule() {
        Ok(bup) => bup,
        Err(e) => panic!("{:?}", e),
    };

    assert_eq!(0, buffer_list.len());
}

// #[test]
// fn test_buffer_up_update() {
//     // Update row
//     // Compare Informations
// }

// #[test]
// fn test_buffer_up_delete() {
//     // Delete row create previously
//     // Get Data
//     // Verify delte
// }

use crate::common::helpers::functions::remove_directory;

// #[test]
// fn test_buffer_up_capacity() {
//     // TODO >>> Test large storage with multiple itens and compare results

//     // Remove the temp db after the tests:
//     remove_directory("./Temp/");
// }
