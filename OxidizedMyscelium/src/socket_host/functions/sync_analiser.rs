use core::panic;
use std::collections::HashMap;

use crate::NodeStatus;
use rusqlite::types::Value;

use crate::{common::client_manager::manager::get_all_clients, ClientError, NetworkMap, Node, CLIENTS_SYNC_CONTROLLER, HOST_COMMAND_PATTERNS};

pub async fn sync_verifier() {
    // -> Try to get the clients registred in the database
    let mut clients = match get_all_clients().await {
        Ok(c) => c,
        Err(e) => {
            panic!("Error getting clients in the sync analiser")
        }, // handle this error case
    };

    let mut actual_patterns: NetworkMap = NetworkMap::new(Vec::new());

    // Get the global network:
    {
        let command_patterns = HOST_COMMAND_PATTERNS.lock().await;
        actual_patterns = command_patterns.clone()
    }

    let mut cli_nodes = actual_patterns.get_all_nodes_except_node_with_key(&"".to_string());
    let mut node_map: HashMap<String, NodeStatus> = HashMap::new();

    for cli_node in &mut cli_nodes {
        if let Some(key) = cli_node.key.clone() {
            node_map.insert(key, cli_node.get_node_status());
        };
    }

    for client in clients {
        let cli_status = node_map.get(&client.client_key).unwrap();

        if (*cli_status == NodeStatus::NotImplemented || *cli_status == NodeStatus::Offline) || *cli_status == NodeStatus::NotSyncYet {
            continue; // -> We don't have any reasons to check node sync status for these cases (this will save hardware ressources)
        }

        let mut expected_know_network: Vec<Node> = actual_patterns.get_all_nodes_except_node_with_key(&client.client_key);

        // Erase the known network of the nodes here just for comparison, this evoids infinite nested known network entities
        for node in &mut expected_know_network {
            node.erase_known_network()
        }

        let comparison_node = actual_patterns.get_node_by_key(&client.client_key).unwrap();
        if comparison_node.network_know_differ(&Some(expected_know_network.clone())) {
            //> If the network know by the client is diferent of what is should be, then change
            //> the sync controller status to not Sync without change the client status to NotSyncYet
            //> since this will be done automatically by the sync controller mechanism in the socket_host
            //> if for some reason client refuse to sync in halth of the max sync allowed.

            println!(
                "\n\n[SYNC ANALISER][HOST] - Node {:?}, isn't sync with current network!\nCurrent network {:?}\nExpected: {:?}",
                &client.client_key,
                comparison_node.get_known_network(),
                expected_know_network
            );

            {
                let mut controller = CLIENTS_SYNC_CONTROLLER.lock().await;
                controller.get_client(&client.client_key).unwrap().update_sync_status(false);
                client.change_sync_to(false);
                client.save_into_db();
                // This should trigger sync to the client
            }
        } else {
            {
                let mut controller = CLIENTS_SYNC_CONTROLLER.lock().await;
                controller.get_client(&client.client_key).unwrap().update_sync_status(true);
                client.change_sync_to(true);
                client.save_into_db();
                // This should trigger sync to the client
            }
        };
    }
}
