use indexmap::IndexMap;
use rusqlite::types::Value;

use crate::{
    common::enhanced_buffer::{
        buffer_down_manager::DownCommand,
        utilities::{CommandMode, CommandOrigin, CommandStatus, CommandTarget, ResponseTarget, ResponseType},
    },
    set_host_callbacks,
    socket_host::transposer::process,
    Command, CommandInstructions, CommandType, HandlerStatus, NetworkMap, Node, NodeHandler, NodeStatus, NodeVersion, VersionIndentifier, HOST_COMMAND_PATTERNS,
};
use core::panic;
use oxidized_myscelium_macros::callback;
use std::any::Any;
use std::collections::HashMap;

use crate::socket_host::command_handler::redirect_commands_processing;
use crate::socket_host::tests::test_transposer::uni_setup::setup_once;
use crate::socket_host::tests::utilities::functions::registry_handlers;
use crate::{CallbackClosure, FunctionMetadata};

#[test]
fn test_redirect_command_response_pointing_to_origin() {
    // -> Here is were we setup our callbacks that we will use in the tests
    setup_once();

    // < This command that should send the command to target: `otherclientid` is activating a function in Host

    let mut instructions: CommandInstructions = CommandInstructions {
        mode: CommandMode::Function,
        command_type: CommandType::ExternalFunction,
        target: CommandTarget::ClientKey("otherclientid".to_string()),
        status: CommandStatus::Success,
        origin: CommandOrigin::ClientKey("someclientid".to_string()),
        actf: "redirect_actf".to_string(),
        kwargs: HashMap::new(),
        message: "some_message".to_string(),
        response_type: Some(ResponseType::ExternalFunction),
        response_target: Some(ResponseTarget::Origin),
        response_actf: Some("some_response_actf".to_string()),
        collect_response: true,
    };

    let command = Command::new("someclientid".to_string(), "xNmlMpN34x14s".to_string(), 1u8, instructions.clone());

    let target = match &command.command.target {
        CommandTarget::ClientKey(c) => c,
        _ => {
            panic!("Target not supported to this test!")
        },
    };

    // TODO >>> Add the required nodes to this test do what it is supposed to do.

    let mut nodes: Vec<Node> = Vec::new();

    // -> Registry Node "otherclientid"
    let mut node_handlers: Vec<NodeHandler> = Vec::new();

    node_handlers.push(NodeHandler::new(
        "redirect_actf".to_string(),
        IndexMap::new(),
        CommandType::ExternalFunction,
        HandlerStatus::Working,
        HashMap::new(),
        "".to_string(),
    ));

    nodes.push(Node::new(
        "Client2".to_string(),
        "otherclientid".to_string(),
        "".to_string(),
        NodeVersion::cast_version(1u32, 3u32, 1u32, VersionIndentifier::ReleaseCandidate),
        node_handlers,
        NodeStatus::Online,
    ));

    let mut network_map = NetworkMap::new(nodes);

    // -> Test responses
    let response = redirect_commands_processing(&command, target, &mut network_map);

    println!("processed: {:?}", response);

    // let down_command = DownCommand::from_command(command);
    // let processed_command = process(down_command);

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

    // TODO >>> Verify why this isn't redirecting, eveen that all the base tests are passing

    // let expected_response = Command::new("someclientid".to_string(), "xNmlMpN34x14s".to_string(), 1u8, response_expected_instructions.clone());

    // println!("processed: {:?}", processed_command);

    assert_eq!(2 + 1, 4);
}
