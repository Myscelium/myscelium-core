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

use crate::{CallbackClosure, FunctionMetadata};

pub async fn registry_handlers(handlers: Vec<FunctionMetadata>) {
    let mut node_handlers: Vec<NodeHandler> = Vec::new();
    let mut callbacks_patterns: HashMap<String, Box<CallbackClosure>> = HashMap::new();

    for handler in handlers {
        println!(
            "Registred handler: {} with args: {:?}",
            handler.name, handler.args
        );

        node_handlers.push(NodeHandler::new(
            handler.name.to_string(),
            handler.args,
            CommandType::ExternalFunction,
            HandlerStatus::NotTested,
            HashMap::new(),
            "".to_string(),
        ));
        callbacks_patterns.insert(handler.name.to_string(), Box::new(handler.func));
    }

    set_host_callbacks(callbacks_patterns).await;

    //> Add Client
    {
        let mut args_types_value: IndexMap<String, String> = IndexMap::new();

        args_types_value.insert("client_name".to_string(), "str".to_string());
        args_types_value.insert("client_key".to_string(), "str".to_string());
        args_types_value.insert("client_type".to_string(), "str".to_string());
        args_types_value.insert("permission_group".to_string(), "str".to_string());
        args_types_value.insert("is_super_user".to_string(), "bool".to_string());
        args_types_value.insert("max_sub_channels".to_string(), "int".to_string());
        args_types_value.insert("owned_sub_channels_keys".to_string(), "list".to_string());

        let host_add_client_handler: NodeHandler = NodeHandler::new(
            "add_client".to_string(),
            args_types_value.clone(),
            CommandType::DirectFunction,
            HandlerStatus::Working,
            HashMap::new(),
            "".to_string(),
        );
        node_handlers.push(host_add_client_handler);
    }

    //> Update Client
    {
        let mut args_types_value: IndexMap<String, String> = IndexMap::new();
        args_types_value.insert("actual_client_key".to_string(), "str".to_string());
        args_types_value.insert("updated_client".to_string(), "dict".to_string());
        // TODO >>> make be possible to do sub dict explicity definitions of the parameters that is should have, also do the same with lists too
        let host_update_client_handler: NodeHandler = NodeHandler::new(
            "update_client".to_string(),
            args_types_value.clone(),
            CommandType::DirectFunction,
            HandlerStatus::Working,
            HashMap::new(),
            "".to_string(),
        );
        node_handlers.push(host_update_client_handler);
    }

    //> Remove Client
    {
        let mut args_types_value: IndexMap<String, String> = IndexMap::new();
        args_types_value.insert("client_key".to_string(), "str".to_string());
        let host_remove_client_handler: NodeHandler = NodeHandler::new(
            "remove_client".to_string(),
            args_types_value.clone(),
            CommandType::DirectFunction,
            HandlerStatus::Working,
            HashMap::new(),
            "".to_string(),
        );
        node_handlers.push(host_remove_client_handler);
    }

    // -> UPDATE HOST NODE WITH THE HANDLERS

    let mut global_command_patterns = HOST_COMMAND_PATTERNS.lock().await;
    let node_version = NodeVersion::cast_version(1, 3, 0, VersionIndentifier::ReleaseCandidate);
    let host_node: Node = Node::new(
        "host".to_string(),
        "host".to_string(),
        "".to_string(),
        node_version,
        node_handlers,
        NodeStatus::Online,
    );
    global_command_patterns.add_or_update_if_exists(host_node);
}
