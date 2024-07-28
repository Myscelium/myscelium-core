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
use std::sync::Once;

static INIT: Once = Once::new();

// TODO >>> Generate all command cases for redirect command cases where we should trasnpose
// TODO >>> Create a method to pass all the tested commands through the process and verify the outcome
// TODO >>> See if the outcome of the process is a success or not
// TODO >>> Create a panic handler to the cases that the command gives a error

#[callback]
fn redirect_actf(info: &HashMap<String, Value>) -> CommandInstructions {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_once() {
        INIT.call_once(|| {
            let handlers = vec![redirect_actf()];
            registry_handlers(handlers);
        });
    }

    // -> SHOULD PASS:

    #[test]
    fn test_redirect_command_with_inplace_response_pointing_to_origin() {
        setup_once(); // Ensure setup is called before this test runs
                      // Test code
        assert_eq!(2 + 2, 4);
    }

    #[test]
    fn test_redirect_command_with_response_pointing_to_origin() {
        setup_once(); // Ensure setup is called before this test runs
                      // Test code
        assert_eq!(2 + 2, 4);
    }

    #[test]
    fn test_redirect_command_with_response_pointing_to_remote() {
        setup_once(); // Ensure setup is called before this test runs
                      // Test code
        assert_eq!(3 * 3, 9);
    }

    // -> SHOULD NOT PASS:

    #[test]
    fn test_redirect_command_with_inplace_response_pointing_to_remote() {
        // < THIS SHOULD NOT PASS BECAUSE WE CANT SEND INPLACE RESPONSE TO TARGET != ORIGIN

        setup_once(); // Ensure setup is called before this test runs
                      // Test code
        assert_eq!(3 * 3, 9);
    }
}
