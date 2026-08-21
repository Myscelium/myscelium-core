// SPDX-License-Identifier: MPL-2.0
// Copyright © 2021-2026 Cristian Camargo Filho

use crate::common::client_manager::manager::{check_if_client_key_exists, Client, ClientError};
use crate::common::enhanced_buffer;
use crate::common::enhanced_buffer::buffer_down_manager::DownCommand;
use crate::common::enhanced_buffer::buffer_up_manager::UpCommand;
use crate::common::enhanced_buffer::utilities::{
    Command, CommandInstructions, CommandMode, CommandOrigin, CommandStatus, CommandTarget,
    CommandType,
};
use crate::common::enhanced_buffer::utilities::{ResponseTarget, ResponseType};
use crate::common::functions::converters::convert_value_map_to_resulttype_map;
use crate::common::functions::converters::ConversionError;
use crate::common::structs::results_structs::ResultType;
use crate::common::types::SchedulingError;
use serde_json::to_string;

use lazy_static::lazy_static;
use serde_json::{from_str, Value};
use std::collections::HashMap;

use crate::HOST_COMMAND_PATTERNS;

use serde_json::Error;

use crate::common::structs::available_commands::{CommandPatterns, NetworkMap, Node};

use super::host_logger::log_handler::Logger;
use crate::socket_host::transposer::process_map_result;
use crate::HOST_LOG_LEVEL;

macro_rules! acquire_logger {
    ($section_name:expr) => {{
        let client_log_level;
        {
            client_log_level = HOST_LOG_LEVEL.lock().await.clone();
        }
        Logger::new(client_log_level, $section_name).await
    }};
}

/// Sets the global client ID to the specified value.
///
/// The client ID is a unique identifier that represents the client in the communication process.
/// This function updates the global `CLIENT_ID` variable to the provided value.
///
/// # Arguments
/// - `client_uid`: The new client ID to be set.
// pub fn set_host_id(client_uid: String) { // -> Future impl
//     println!("Setting client_key to: {:?}", client_uid.clone());

//     {
//         let mut client_id_global = CLIENT_ID.lock();
//         *client_id_global = client_uid.clone();
//     }
// }

/// Requests the available commands that are registered on the host.
///
/// This function prepares a command request for the host to retrieve the list of
/// registered commands. The constructed request is then scheduled for processing.
pub async fn request_client_available_commands(client_key: String) -> Result<(), SchedulingError> {
    let command_instructions: CommandInstructions = CommandInstructions::new(
        CommandMode::Function,
        CommandType::DirectFunction,
        CommandTarget::Origin, // Returns to client_key of the command (the owner of the command)
        CommandStatus::Success,
        CommandOrigin::Host, // -> This will be an identifier, to know the origin of the retransmited command
        "get_socket_client_available_handlers".to_string(),
        HashMap::new(),
        "".to_string(),
        Some(ResponseType::DirectFunction),
        Some(ResponseTarget::Host),
        Some("update_client_commands_ref".to_string()),
        true,
    );

    schedule(
        &command_instructions,
        11,
        client_key,
        "itisaspecialcase".to_string(),
    )
    .await
}

pub async fn send_network_available_commands(client_key: String) -> Result<(), SchedulingError> {
    let logger = acquire_logger!("Scheduler");

    // TODO >>> Update the network known in the node since we are sending the new network know

    // -> CONSIDERATIONS:

    //> This will guarantee that the node is sync because the node will have the sync mark in sync controler
    //> when a node is defined as not sync in the controller it will attempt to sync eveen if the network known
    //> status is updted since now it is marked as not sync yet. Also if this isn't syncing the controller
    //> changes the sync status to NotSyncYet, if it persists it will be changed to Offline.

    logger
        .info(format!(
            "Send update_available_host_commands to client trying to sync!"
        ))
        .await;

    // Lock the HOST_COMMAND_PATTERNS and insert the new map

    let mut filtered_commands: HashMap<String, Value> = HashMap::new();

    {
        let mut command_patterns = HOST_COMMAND_PATTERNS.lock().await;

        // -> Get the known network that this node should know (updated one)
        let mut nodes: Vec<Node> = command_patterns.get_all_nodes_except_node_with_key(&client_key);

        // -> Update the known network of this node
        let mut actual_node = command_patterns.get_node_by_key(&client_key).unwrap();
        actual_node.update_known_network(nodes.clone());

        //> Erase the network know of the nodes since this info is restrict to host and not need to be sended to the client
        for node in &mut nodes {
            node.erase_known_network();
        }

        // -> Save to deliver to the node
        filtered_commands.insert(
            "network_nodes".to_string(),
            serde_json::to_value(nodes).unwrap(),
        );
    }

    //> Sync mechanism don't uses the response_actf dinamic system cause it is splicity configured and has diferent cases in the sync flow
    //> for example in the first sync the `update_available_host_commands` response is: `update_client_commands_ref` the consectuvie times
    //> is

    let command_instructions = CommandInstructions::new(
        CommandMode::Function,
        CommandType::DirectFunction,
        CommandTarget::Origin, // Returns to client_key of the command (the owner of the command)
        CommandStatus::Success,
        CommandOrigin::Host,
        "update_available_host_commands".to_string(),
        filtered_commands,
        "".to_string(),
        Some(ResponseType::DirectFunction),
        Some(ResponseTarget::Host),
        None, // Not required in this case
        true,
    );

    schedule(
        &command_instructions,
        11u8,
        client_key,
        "itisaspecialcase".to_string(),
    )
    .await
}

/// Schedules a command for processing.
///
/// The function takes in a command and its priority, then schedules it for processing
/// by converting the command to a string, generating a unique parity ID, and adding it
/// to the up buffer manager's schedule.
///
/// # Arguments
/// - `command`: A map representing the command to be scheduled.
/// - `priority`: The priority level of the command. Commands with higher priority values
///               are processed before those with lower priority values.
pub async fn schedule(
    command: &CommandInstructions,
    priority: u8,
    client_key: String,
    parity_id: String,
) -> Result<(), SchedulingError> {
    let response: Value;
    let new_client_key: String;

    (response, new_client_key) =
        process_map_result(&command, &client_key, &parity_id, &priority, &None).await;
    let logger = acquire_logger!("Core - Scheduler");

    logger.debug("Enter Scheduler".to_string()).await;

    let parity_id =
        enhanced_buffer::buffer_up_manager::buffer_up_gen_valid_parity_id(client_key.clone())
            .await?;
    let command_to_schedule = UpCommand::new(
        &new_client_key,
        &parity_id,
        priority,
        &to_string(&response).unwrap(),
    );
    enhanced_buffer::buffer_up_manager::buffer_up_schedule(command_to_schedule.clone())
        .await
        .map_err(|e| SchedulingError::from(e))?;

    logger
        .info(format!("Command: {:?} scheduled!", command_to_schedule))
        .await;

    Ok(())
}
