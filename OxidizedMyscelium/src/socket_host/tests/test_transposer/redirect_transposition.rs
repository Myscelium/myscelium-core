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

use crate::socket_host::tests::test_transposer::uni_setup::setup_once;
use crate::socket_host::tests::utilities::functions::registry_handlers;
use crate::{CallbackClosure, FunctionMetadata};

// TODO >>> Generate all command cases for redirect command cases where we should trasnpose
// TODO >>> Create a method to pass all the tested commands through the process and verify the outcome
// TODO >>> See if the outcome of the process is a success or not
// TODO >>> Create a panic handler to the cases that the command gives a error

#[cfg(test)]
mod tests {
    use super::*;

    // -> SHOULD PASS:

    #[test]
    fn test_redirect_command_response_pointing_to_origin() {
        // -> Here is were we setup our callbacks that we will use in the tests
        setup_once();

        let mut instructions: CommandInstructions = CommandInstructions {
            mode: CommandMode::Function,
            command_type: CommandType::ExternalFunction,
            target: CommandTarget::ClientKey("otherclientid".to_string()),
            status: CommandStatus::Success,
            origin: CommandOrigin::Host,
            actf: "redirect_actf".to_string(),
            kwargs: HashMap::new(),
            message: "some_message".to_string(),
            response_type: Some(ResponseType::ExternalFunction),
            response_target: Some(ResponseTarget::Origin),
            response_actf: Some("some_response_actf".to_string()),
            collect_response: true,
        };

        let command = Command::new("someclientid".to_string(), "xNmlMpN34x14s".to_string(), 1u8, instructions.clone());
        let down_command = DownCommand::from_command(command);
        let processed_command = process(down_command);

        // let mut response_expected_instructions: CommandInstructions = CommandInstructions {
        //     mode: CommandMode::Response,
        //     command_type: CommandType::ExternalFunction,
        //     target: CommandTarget::Origin,
        //     status: CommandStatus::Success,
        //     origin: CommandOrigin::Host,
        //     actf: "some_actf".to_string(),
        //     kwargs: HashMap::new(),
        //     message: "Hello".to_string(),
        //     response_type: None,
        //     response_target: None,
        //     response_actf: None,
        //     collect_response: true,
        // };

        // let expected_response = Command::new("someclientid".to_string(), "xNmlMpN34x14s".to_string(), 1u8, response_expected_instructions.clone());

        println!("processed: {:?}", processed_command);

        assert_eq!(2 + 1, 4);
    }

    #[test]
    fn test_redirect_command_with_inplace_response_pointing_to_origin() {
        // -> Here is were we setup our callbacks that we will use in the tests
        setup_once();
        assert_eq!(2 + 2, 4);
    }

    #[test]
    fn test_redirect_command_with_response_pointing_to_remote() {
        // -> Here is were we setup our callbacks that we will use in the tests
        setup_once();
        assert_eq!(3 * 3, 9);
    }

    // -> SHOULD NOT PASS:

    #[test]
    fn test_redirect_command_with_inplace_response_pointing_to_remote() {
        // < THIS SHOULD NOT PASS BECAUSE WE CANT SEND INPLACE RESPONSE TO TARGET != ORIGIN

        // -> Here is were we setup our callbacks that we will use in the tests
        setup_once();
        assert_eq!(3 * 3, 9);
    }
}
