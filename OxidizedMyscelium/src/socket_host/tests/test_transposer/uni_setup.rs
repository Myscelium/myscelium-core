// SPDX-License-Identifier: MPL-2.0
// Copyright © 2021-2026 Cristian Camargo Filho

use indexmap::IndexMap;
use rusqlite::types::Value;

use crate::{
    common::enhanced_buffer::{
        buffer_down_manager::DownCommand,
        utilities::{
            CommandMode, CommandOrigin, CommandStatus, CommandTarget, ResponseTarget, ResponseType,
        },
    },
    set_host_callbacks,
    socket_host::transposer::process,
    Command, CommandInstructions, CommandType, HandlerStatus, Node, NodeHandler, NodeStatus,
    NodeVersion, VersionIndentifier, HOST_COMMAND_PATTERNS,
};
use core::panic;
use oxidized_myscelium_macros::callback;
use std::any::Any;
use std::collections::HashMap;

use crate::socket_host::tests::utilities::functions::registry_handlers;
use crate::{CallbackClosure, FunctionMetadata};

use std::sync::Once;
static INIT: Once = Once::new();

// -> Basic transposition test callbacks:

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

// -> Redirect transposition test callbacks:

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

// -> Setup:

pub fn setup_once() {
    INIT.call_once(|| {
        let mut handlers: Vec<FunctionMetadata> = Vec::new();

        // -> Integrate Basic transposition Test Callbacks:
        let basic_test_callbacks = vec![some_actf()];
        handlers.extend(basic_test_callbacks);

        // -> Integrate Redirect Transposition Test Callbacks:
        let redirect_test_callbacks = vec![redirect_actf()];
        handlers.extend(redirect_test_callbacks);

        registry_handlers(handlers);
    });
}
