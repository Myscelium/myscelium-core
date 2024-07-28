use indexmap::IndexMap;
use rusqlite::types::Value;

use crate::{
    common::enhanced_buffer::{
        buffer_down_manager::DownCommand,
        utilities::{CommandMode, CommandOrigin, CommandStatus, CommandTarget, ResponseTarget, ResponseType},
    },
    set_host_callbacks,
    socket_host::transposer::process,
    Command, CommandInstructions, CommandType, HandlerStatus, Node, NodeHandler, NodeStatus, NodeVersion, VersionIndentifier, HOST_COMMAND_PATTERNS,
};
use core::panic;
use oxidized_myscelium_macros::callback;
use std::any::Any;
use std::collections::HashMap;

use crate::socket_host::tests::utilities::functions::registry_handlers;
use crate::{CallbackClosure, FunctionMetadata};

#[callback]
fn some_actf(info: &HashMap<String, Value>) -> CommandInstructions {
    let response = CommandInstructions {
        mode: CommandMode::Response,
        command_type: CommandType::ExternalFunction,
        target: CommandTarget::Origin,
        status: CommandStatus::Success,
        origin: CommandOrigin::Host,
        actf: "some_actf".to_string(),
        kwargs: HashMap::new(),
        message: "Hello".to_string(),
        response_type: None,
        response_target: None,
        response_actf: None,
        collect_response: true,
    };

    println!("Resp: {:?}", response);
    response
}

#[derive(Clone)]
struct Rules {
    command_modes: Vec<CommandMode>,
    command_types: Vec<CommandType>,
    command_targets: Vec<CommandTarget>,
    response_targets: Vec<Option<ResponseTarget>>,
    command_statuses: Vec<CommandStatus>,
    command_origins: Vec<CommandOrigin>,
    response_types: Vec<ResponseType>,
    auto_collect_options: Vec<bool>,
}

fn generate_combinations_recursive(current: &mut CommandInstructions, index: usize, fields: &[Vec<(String, Box<dyn Fn(&mut CommandInstructions, String)>)>], rules: &Rules, valid_combinations: &mut Vec<CommandInstructions>) {
    if index == fields.len() {
        if validate_command_instruction(current, rules) {
            valid_combinations.push(current.clone());
        }
        return;
    }

    for (value, setter) in &fields[index] {
        setter(current, value.clone());
        generate_combinations_recursive(current, index + 1, fields, rules, valid_combinations);
    }
}

fn validate_command_instruction(instruction: &CommandInstructions, rules: &Rules) -> bool {
    match instruction.mode {
        CommandMode::Function => {
            if let CommandTarget::ClientKey(_) = &instruction.target {
                if let Some(ResponseTarget::Origin) = &instruction.response_target {
                    if !instruction.collect_response {
                        return true;
                    }
                } else if let Some(ResponseTarget::Host) = &instruction.response_target {
                    if instruction.collect_response {
                        return true;
                    }
                } else if let Some(ResponseTarget::ClientKey(_)) = &instruction.response_target {
                    if !instruction.collect_response {
                        return true;
                    }
                }
            } else if let CommandTarget::Host = &instruction.target {
                if let Some(ResponseTarget::Origin) = &instruction.response_target {
                    if !instruction.collect_response {
                        return true;
                    }
                } else if let Some(ResponseTarget::Host) = &instruction.response_target {
                    if instruction.collect_response {
                        return true;
                    }
                } else if let Some(ResponseTarget::ClientKey(_)) = &instruction.response_target {
                    if !instruction.collect_response {
                        return true;
                    }
                }
            }
        },
        CommandMode::Response => {
            if let CommandTarget::ClientKey(_) = &instruction.target {
                if instruction.collect_response {
                    return true;
                }
            } else if let CommandTarget::Host = &instruction.target {
                if !instruction.collect_response {
                    return true;
                }
            }
        },
    }
    false
}

#[test]
fn test_down_command_transposition() {
    let handlers = vec![some_actf()];
    registry_handlers(handlers);

    // -> INITIALIZE RULES:

    let rules_function = Rules {
        command_modes: vec![CommandMode::Function],
        command_types: vec![CommandType::SpecialFunction, CommandType::DirectFunction, CommandType::ExternalFunction],
        command_targets: vec![CommandTarget::ClientKey("some_client".to_string()), CommandTarget::Host],
        response_targets: vec![Some(ResponseTarget::Origin), Some(ResponseTarget::Host), Some(ResponseTarget::ClientKey("some_client".to_string()))],
        command_statuses: vec![CommandStatus::Success, CommandStatus::Failure],
        command_origins: vec![CommandOrigin::Host, CommandOrigin::ClientKey("some_client".to_string())],
        response_types: vec![ResponseType::DirectFunction, ResponseType::ExternalFunction],
        auto_collect_options: vec![true, false],
    };

    let rules_response = Rules {
        command_modes: vec![CommandMode::Response],
        command_types: vec![CommandType::DirectFunction, CommandType::ExternalFunction],
        command_targets: vec![CommandTarget::ClientKey("some_client".to_string()), CommandTarget::Host],
        response_targets: vec![None],
        command_statuses: vec![CommandStatus::Success, CommandStatus::Failure],
        command_origins: vec![CommandOrigin::Host, CommandOrigin::ClientKey("some_client".to_string())],
        response_types: vec![ResponseType::DirectFunction, ResponseType::ExternalFunction],
        auto_collect_options: vec![true],
    };

    let fields_function: Vec<Vec<(String, Box<dyn Fn(&mut CommandInstructions, String)>)>> = vec![
        vec![("Function".to_string(), Box::new(|c: &mut CommandInstructions, _| c.mode = CommandMode::Function))],
        vec![
            // ("SpecialFunction".to_string(), Box::new(|c: &mut CommandInstructions, _| c.command_type = CommandType::SpecialFunction)),
            // ("DirectFunction".to_string(), Box::new(|c: &mut CommandInstructions, _| c.command_type = CommandType::DirectFunction)),
            ("ExternalFunction".to_string(), Box::new(|c: &mut CommandInstructions, _| c.command_type = CommandType::ExternalFunction)),
        ],
        vec![
            ("ClientKey(some_client)".to_string(), Box::new(|c: &mut CommandInstructions, v| c.target = CommandTarget::ClientKey(v))),
            ("Host".to_string(), Box::new(|c: &mut CommandInstructions, _| c.target = CommandTarget::Host)),
        ],
        vec![
            ("Origin".to_string(), Box::new(|c: &mut CommandInstructions, _| c.response_target = Some(ResponseTarget::Origin))),
            ("Host".to_string(), Box::new(|c: &mut CommandInstructions, _| c.response_target = Some(ResponseTarget::Host))),
            ("ClientKey(some_client)".to_string(), Box::new(|c: &mut CommandInstructions, v| c.response_target = Some(ResponseTarget::ClientKey(v)))),
        ],
        vec![
            ("Success".to_string(), Box::new(|c: &mut CommandInstructions, _| c.status = CommandStatus::Success)),
            ("Failure".to_string(), Box::new(|c: &mut CommandInstructions, _| c.status = CommandStatus::Failure)),
        ],
        vec![
            // ("DirectFunction".to_string(), Box::new(|c: &mut CommandInstructions, _| c.response_type = Some(ResponseType::DirectFunction))),
            ("ExternalFunction".to_string(), Box::new(|c: &mut CommandInstructions, _| c.response_type = Some(ResponseType::ExternalFunction))),
        ],
        vec![
            ("Host".to_string(), Box::new(|c: &mut CommandInstructions, _| c.origin = CommandOrigin::Host)),
            ("ClientKey(some_client)".to_string(), Box::new(|c: &mut CommandInstructions, v| c.origin = CommandOrigin::ClientKey(v))),
        ],
    ];

    let fields_response: Vec<Vec<(String, Box<dyn Fn(&mut CommandInstructions, String)>)>> = vec![
        vec![("Response".to_string(), Box::new(|c: &mut CommandInstructions, _| c.mode = CommandMode::Response))],
        vec![
            ("DirectFunction".to_string(), Box::new(|c: &mut CommandInstructions, _| c.command_type = CommandType::DirectFunction)),
            ("ExternalFunction".to_string(), Box::new(|c: &mut CommandInstructions, _| c.command_type = CommandType::ExternalFunction)),
        ],
        vec![
            ("ClientKey(some_client)".to_string(), Box::new(|c: &mut CommandInstructions, v| c.target = CommandTarget::ClientKey(v))),
            ("Host".to_string(), Box::new(|c: &mut CommandInstructions, _| c.target = CommandTarget::Host)),
        ],
        vec![(String::new(), Box::new(|c: &mut CommandInstructions, _| c.response_target = None))],
        vec![
            ("Success".to_string(), Box::new(|c: &mut CommandInstructions, _| c.status = CommandStatus::Success)),
            ("Failure".to_string(), Box::new(|c: &mut CommandInstructions, _| c.status = CommandStatus::Failure)),
        ],
        vec![
            ("DirectFunction".to_string(), Box::new(|c: &mut CommandInstructions, _| c.response_type = Some(ResponseType::DirectFunction))),
            ("ExternalFunction".to_string(), Box::new(|c: &mut CommandInstructions, _| c.response_type = Some(ResponseType::ExternalFunction))),
        ],
        vec![
            ("Host".to_string(), Box::new(|c: &mut CommandInstructions, _| c.origin = CommandOrigin::Host)),
            ("ClientKey(some_client)".to_string(), Box::new(|c: &mut CommandInstructions, v| c.origin = CommandOrigin::ClientKey(v))),
        ],
    ];

    let mut valid_combinations = Vec::new();
    let mut initial_command = CommandInstructions {
        mode: CommandMode::Function,
        command_type: CommandType::SpecialFunction,
        target: CommandTarget::Origin,
        status: CommandStatus::Success,
        origin: CommandOrigin::Host,
        actf: "some_actf".to_string(),
        kwargs: HashMap::new(),
        message: "some_message".to_string(),
        response_type: Some(ResponseType::DirectFunction),
        response_target: Some(ResponseTarget::Origin),
        response_actf: Some("some_response_actf".to_string()),
        collect_response: true,
    };

    // -> TEST ALL VALID COMMANDS:

    generate_combinations_recursive(&mut initial_command, 0, &fields_function, &rules_function, &mut valid_combinations);
    generate_combinations_recursive(&mut initial_command, 0, &fields_response, &rules_response, &mut valid_combinations);

    let mut total_responses: u64 = 0u64;
    let mut total_failures: u64 = 0u64;

    println!("Generated {} valid CommandInstructions instances.", valid_combinations.len());
    for (i, instruction) in valid_combinations.iter().enumerate() {
        println!("C{:?}: \n{:?}\n", i, instruction);

        let command = Command::new("someclientid".to_string(), "xNmlMpN34x14s".to_string(), 1u8, instruction.clone());
        let down_command = DownCommand::from_command(command);
        let responses = process(down_command);

        println!("Response:\n{:?}\n", responses);

        for response in responses {
            let com = Command::from_up_command(&response.clone()).unwrap();
            if com.command.status == "Failure" {
                total_failures += 1;
            }
            total_responses += 1;
        }

        println!("\n{} responses and {} failed", total_responses, total_failures);
        if total_failures > 1 {
            panic!("{} command functions/responses failed", total_failures);
        }
    }

    // -> TEST ALL VALID RESPONSES:

    // let mut valid_combinations = Vec::new();
    // generate_combinations_recursive(&mut initial_command, 0, &fields_response, &rules_response, &mut valid_combinations);

    // panic!();

    // let mut down_commands: Vec<DownCommand> = Vec::new();

    // // -> ALL FIELDS PRESENT:

    // down_commands.push(DownCommand {
    //     command_id: Some(1), // Or any other u32 value
    //     client_key: "some_client_key".to_string(),
    //     parity_id: "some_parity_id".to_string(),
    //     priority: 1,                         // Any valid u8 value
    //     command: "some_command".to_string(), // A command string that can be converted to CommandInstructions
    //     command_mode: CommandMode::Function, // Replace with a valid CommandMode variant
    //     created_time: 1627483647.0,          // Some timestamp
    //     auto_collect: true,                  // Or false
    // });

    // // -> With Optional command_id as None:

    // down_commands.push(DownCommand {
    //     command_id: None,
    //     client_key: "some_client_key".to_string(),
    //     parity_id: "some_parity_id".to_string(),
    //     priority: 1,                         // Any valid u8 value
    //     command: "some_command".to_string(), // A command string that can be converted to CommandInstructions
    //     command_mode: CommandMode::Function, // Replace with a valid CommandMode variant
    //     created_time: 1627483647.0,          // Some timestamp
    //     auto_collect: true,                  // Or false
    // });

    // // -> With Different priority Values:

    // down_commands.push(DownCommand {
    //     command_id: Some(1), // Or None
    //     client_key: "some_client_key".to_string(),
    //     parity_id: "some_parity_id".to_string(),
    //     priority: 0,                         // Minimum valid u8 value
    //     command: "some_command".to_string(), // A command string that can be converted to CommandInstructions
    //     command_mode: CommandMode::Function, // Replace with a valid CommandMode variant
    //     created_time: 1627483647.0,          // Some timestamp
    //     auto_collect: true,                  // Or false
    // });

    // down_commands.push(DownCommand {
    //     command_id: Some(1), // Or None
    //     client_key: "some_client_key".to_string(),
    //     parity_id: "some_parity_id".to_string(),
    //     priority: 255,                       // Maximum valid u8 value
    //     command: "some_command".to_string(), // A command string that can be converted to CommandInstructions
    //     command_mode: CommandMode::Function, // Replace with a valid CommandMode variant
    //     created_time: 1627483647.0,          // Some timestamp
    //     auto_collect: true,                  // Or false
    // });

    // // -> With Different command_mode Variants:

    // down_commands.push(DownCommand {
    //     command_id: Some(1), // Or None
    //     client_key: "some_client_key".to_string(),
    //     parity_id: "some_parity_id".to_string(),
    //     priority: 1,                         // Any valid u8 value
    //     command: "some_command".to_string(), // A command string that can be converted to CommandInstructions
    //     command_mode: CommandMode::Function, // Example variant, replace with actual variants
    //     created_time: 1627483647.0,          // Some timestamp
    //     auto_collect: true,                  // Or false
    // });

    // down_commands.push(DownCommand {
    //     command_id: Some(1), // Or None
    //     client_key: "some_client_key".to_string(),
    //     parity_id: "some_parity_id".to_string(),
    //     priority: 1,                         // Any valid u8 value
    //     command: "some_command".to_string(), // A command string that can be converted to CommandInstructions
    //     command_mode: CommandMode::Function, // Example variant, replace with actual variants
    //     created_time: 1627483647.0,          // Some timestamp
    //     auto_collect: true,                  // Or false
    // });

    // for down_command in down_commands {
    //     let result = process(down_command);
    //     println!("Response: {:?}", result);
    // }
}
