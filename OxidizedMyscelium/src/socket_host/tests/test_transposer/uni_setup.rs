// SPDX-License-Identifier: MPL-2.0
// Copyright © 2021-2026 Cristian Camargo Filho

use indexmap::IndexMap;
use rusqlite::types::Value;
use tokio::sync::OnceCell;

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
static INIT: OnceCell<()> = OnceCell::const_new();

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

pub async fn setup_once() {
    // This function is used to setup tests for:
    // - simple_transposition test
    // - redirect_transposition test

    // get_or_init guarantees the block runs only once, even if many tasks call
    // `setup_once()` concurrently.
    INIT.get_or_init(|| async {
        let mut handlers: Vec<FunctionMetadata> = Vec::new();

        // → Integrate Basic transposition test callbacks
        handlers.extend([some_actf()]);

        // → Integrate Redirect transposition test callbacks
        handlers.extend([redirect_actf()]);

        // `registry_handlers` is now async, so we can await it here.
        registry_handlers(handlers).await;
    })
    .await;
}
