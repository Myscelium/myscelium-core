use crate::{
    common::enhanced_buffer::{buffer_down_manager::DownCommand, utilities::CommandMode},
    socket_host::transposer::process,
};

// TODO >>> Map the commands that are possible to be received in the tranposer process
//> Commands:
//> 1.

// TODO >>> Map the response to each command
//> Responses:
//> 1.

// TODO >>> Map each command with command mode: Response posssible in the transposer process
//> Command Responses:
//> 1.

// TODO >>> Map each command with command mode: Response response obtained in the tranposer process
//> Response of Command Responses:
//> 1.

// TODO >>> Create rules of how each command and response command should behave

// < Maybe the Direct Commands will need some speciall attention cause them change real states
// * In this case od Direct Commands we will need to create a special set of rules to them.

#[test]
fn test_down_command_transposition() {
    let mut down_commands: Vec<DownCommand> = Vec::new();

    // -> ALL FIELDS PRESENT:

    down_commands.push(DownCommand {
        command_id: Some(1), // Or any other u32 value
        client_key: "some_client_key".to_string(),
        parity_id: "some_parity_id".to_string(),
        priority: 1,                         // Any valid u8 value
        command: "some_command".to_string(), // A command string that can be converted to CommandInstructions
        command_mode: CommandMode::Function, // Replace with a valid CommandMode variant
        created_time: 1627483647.0,          // Some timestamp
        auto_collect: true,                  // Or false
    });

    // -> With Optional command_id as None:

    down_commands.push(DownCommand {
        command_id: None,
        client_key: "some_client_key".to_string(),
        parity_id: "some_parity_id".to_string(),
        priority: 1,                         // Any valid u8 value
        command: "some_command".to_string(), // A command string that can be converted to CommandInstructions
        command_mode: CommandMode::Function, // Replace with a valid CommandMode variant
        created_time: 1627483647.0,          // Some timestamp
        auto_collect: true,                  // Or false
    });

    // -> With Different priority Values:

    down_commands.push(DownCommand {
        command_id: Some(1), // Or None
        client_key: "some_client_key".to_string(),
        parity_id: "some_parity_id".to_string(),
        priority: 0,                         // Minimum valid u8 value
        command: "some_command".to_string(), // A command string that can be converted to CommandInstructions
        command_mode: CommandMode::Function, // Replace with a valid CommandMode variant
        created_time: 1627483647.0,          // Some timestamp
        auto_collect: true,                  // Or false
    });

    down_commands.push(DownCommand {
        command_id: Some(1), // Or None
        client_key: "some_client_key".to_string(),
        parity_id: "some_parity_id".to_string(),
        priority: 255,                       // Maximum valid u8 value
        command: "some_command".to_string(), // A command string that can be converted to CommandInstructions
        command_mode: CommandMode::Function, // Replace with a valid CommandMode variant
        created_time: 1627483647.0,          // Some timestamp
        auto_collect: true,                  // Or false
    });

    // -> With Different command_mode Variants:

    down_commands.push(DownCommand {
        command_id: Some(1), // Or None
        client_key: "some_client_key".to_string(),
        parity_id: "some_parity_id".to_string(),
        priority: 1,                         // Any valid u8 value
        command: "some_command".to_string(), // A command string that can be converted to CommandInstructions
        command_mode: CommandMode::Function, // Example variant, replace with actual variants
        created_time: 1627483647.0,          // Some timestamp
        auto_collect: true,                  // Or false
    });

    down_commands.push(DownCommand {
        command_id: Some(1), // Or None
        client_key: "some_client_key".to_string(),
        parity_id: "some_parity_id".to_string(),
        priority: 1,                         // Any valid u8 value
        command: "some_command".to_string(), // A command string that can be converted to CommandInstructions
        command_mode: CommandMode::Function, // Example variant, replace with actual variants
        created_time: 1627483647.0,          // Some timestamp
        auto_collect: true,                  // Or false
    });

    for down_command in down_commands {
        let result = process(down_command);
        println!("Response: {:?}", result);
    }
}
