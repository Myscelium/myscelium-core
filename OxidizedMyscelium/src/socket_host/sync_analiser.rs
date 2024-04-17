use core::panic;
use std::collections::HashMap;

use rusqlite::types::Value;

use crate::{common::client_manager::manager::get_all_clients, handle_manager_client_error, ClientError, NetworkMap, Node, CLIENTS_SYNC_CONTROLLER, HOST_COMMAND_PATTERNS};

pub fn sync_verifier() {
    // -> Try to get the clients registred in the database
    let mut clients = match get_all_clients() {
        Ok(c) => c,
        Err(e) => {
            panic!("Error getting clients in the sync analiser")
        }, // handle this error case
    };

    let mut actual_patterns: NetworkMap = NetworkMap::new(Vec::new());

    // Get the global network:
    {
        let command_patterns = HOST_COMMAND_PATTERNS.lock();
        actual_patterns = command_patterns.clone()
    }

    for client in clients {
        let expected_know_network: Vec<Node> = actual_patterns.get_all_nodes_except_node_with_key(&client.client_key);
        let comparison_node = actual_patterns.get_node_by_key(&client.client_key).unwrap();
        if comparison_node.network_know_differ(&Some(expected_know_network)) {
            //> If the network know by the client is diferent of what is should be, then change
            //> the sync controller status to not Sync without change the client status to NotSyncYet
            //> since this will be done automatically by the sync controller mechanism in the socket_host
            //> if for some reason client refuse to sync in halth of the max sync allowed.
            {
                let mut controller = CLIENTS_SYNC_CONTROLLER.lock();
                controller.get_client(&client.client_key).unwrap().update_sync_status(false)
            }
        };
    }
}
