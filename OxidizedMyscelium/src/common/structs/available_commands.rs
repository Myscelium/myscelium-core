use crate::common::enhanced_buffer::utilities::CommandType;
use crate::common::structs::results_structs::ResultType;
use crate::socket_host::socket_host::ChangeStatusError;

use chrono::{DateTime, Duration, Utc};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

use serde_json::{from_value, Map, Value};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum HandlerStatus {
    Working,
    NotImplemented,
    NotTested,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NodeHandler {
    pub name: String,
    pub parameters: IndexMap<String, String>,
    // It wasn't was changed because this is the only way to directly parse from other languages
    // using bidges that contain json to be a wrapper to the values and translate from one lang to
    // another using it
    handler_type: CommandType,
    status: HandlerStatus,
    response_structure: HashMap<String, Value>,
    description: String,
}

impl NodeHandler {
    pub fn into_hashmap(self) -> HashMap<String, Value> {
        let mut map = HashMap::new();

        // Insert each field into the map. You will need to convert non-String types to Value.
        // This assumes that CommandType and HandlerStatus implement Serialize.
        map.insert("name".to_string(), Value::String(self.name));
        map.insert("parameters".to_string(), serde_json::to_value(self.parameters).unwrap());
        map.insert("handler_type".to_string(), serde_json::to_value(self.handler_type).unwrap());
        map.insert("status".to_string(), serde_json::to_value(self.status).unwrap());
        map.insert("response_structure".to_string(), Value::Object(Map::from_iter(self.response_structure)));
        map.insert("description".to_string(), Value::String(self.description));

        map
    }

    pub fn new(name: String, parameters: IndexMap<String, String>, handler_type: CommandType, status: HandlerStatus, response_structure: HashMap<String, Value>, description: String) -> Self {
        Self {
            name,
            parameters,
            handler_type,
            status,
            response_structure,
            description,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum VersionIndentifier {
    ReleaseCandidate,
    Alpha,
    PreAlpha,
    Beta,
    PreBeta,
    Release,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NodeVersion {
    major: u32,
    minor: u32,
    patch: u32,
    identifier: VersionIndentifier,
}

impl VersionIndentifier {
    pub fn to_string(&self) -> String {
        match &self {
            VersionIndentifier::ReleaseCandidate => "ReleaseCandidate",
            VersionIndentifier::Alpha => "Alpha",
            VersionIndentifier::PreAlpha => "PreAlpha",
            VersionIndentifier::Beta => "Beta",
            VersionIndentifier::PreBeta => "PreBeta",
            VersionIndentifier::Release => "Release",
        }
        .to_string()
    }
}

impl NodeVersion {
    pub fn to_string(&self) -> String {
        format!("{}.{}.{}-{}", self.major, self.minor, self.patch, self.identifier.to_string())
    }
    pub fn cast_version(major: u32, minor: u32, patch: u32, identifier: VersionIndentifier) -> Self {
        Self { major, minor, patch, identifier }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum NodeError {
    InvalidValue,
    NodeNotInitializedYet,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    name: Option<String>,
    pub key: Option<String>,
    status: Option<NodeStatus>,
    description: Option<String>,
    version: Option<NodeVersion>,
    handlers: Option<Vec<NodeHandler>>,
    known_network: Option<Vec<Node>>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum NodeStatus {
    Online,
    Idle,
    NotSyncYet,
    NotImplemented,
    Offline,
}

impl NodeStatus {
    pub fn to_string(&self) -> String {
        match &self {
            NodeStatus::Online => "Online",
            NodeStatus::Offline => "Offline",
            NodeStatus::NotSyncYet => "NotSyncYet",
            NodeStatus::NotImplemented => "NotImplemented",
            NodeStatus::Idle => "Idle",
        }
        .to_string()
    }
}

impl Node {
    pub fn empty_node() -> Self {
        Self {
            name: None,
            key: None,
            status: None,
            description: None,
            version: None,
            handlers: None,
            known_network: None,
        }
    }

    pub fn new(name: String, key: String, description: String, version: NodeVersion, handlers: Vec<NodeHandler>, status: NodeStatus) -> Self {
        Self {
            name: Some(name),
            key: Some(key),
            status: Some(status),
            description: Some(description),
            version: Some(version),
            handlers: Some(handlers),
            known_network: None,
        }
    }

    pub fn partially_initialize(name: String, key: String, status: NodeStatus, description: Option<String>, version: Option<NodeVersion>, handlers: Option<Vec<NodeHandler>>) -> Self {
        Self {
            name: Some(name),
            key: Some(key),
            status: Some(status),
            description: description,
            version: version,
            handlers: handlers,
            known_network: None,
        }
    }

    pub fn get_node_status(&mut self) -> NodeStatus {
        self.status.as_ref().unwrap_or(&NodeStatus::NotImplemented).clone()
    }

    pub fn from_value(value: Value) -> Result<Self, NodeError> {
        let node: Node = match serde_json::from_value(value) {
            Ok(n) => n,
            Err(_) => return Err(NodeError::InvalidValue),
        };
        Ok(node)
    }

    pub fn to_value(&self) -> Value {
        serde_json::to_value(&self).unwrap()
    }

    pub fn get_node_handlers(&self) -> Result<HashMap<String, IndexMap<String, String>>, NodeError> {
        let mut node_handlers: HashMap<String, IndexMap<String, String>> = HashMap::new();

        if let Some(handlers) = &self.handlers {
            for handler in handlers {
                node_handlers.insert(handler.name.clone(), handler.parameters.clone());
            }
        } else {
            return Err(NodeError::NodeNotInitializedYet);
        }

        Ok(node_handlers)
    }

    pub fn change_node_status(&mut self, new_status: NodeStatus) {
        self.status = Some(new_status);
    }

    pub fn update_description(&mut self, description: String) {
        self.description = Some(description)
    }

    pub fn update_name(&mut self, name: String) {
        self.name = Some(name)
    }

    pub fn update_handlers(&mut self, handlers: Vec<NodeHandler>) {
        self.handlers = Some(handlers)
    }

    pub fn update_known_network(&mut self, new_network: Vec<Node>) {
        // -> Turn the sub nodes know know network None to avoid infinite nest
        let mut new_network = new_network;
        for node in &mut new_network {
            node.known_network = None;
        }
        // -> Update the self know network
        self.known_network = Some(new_network);
    }

    pub fn erase_known_network(&mut self) {
        self.known_network = None;
    }

    pub fn update(&mut self, name: String, key: String, description: String, version: NodeVersion, handlers: Vec<NodeHandler>) {
        self.name = Some(name);
        self.key = Some(key);
        self.description = Some(description);
        self.version = Some(version);
        self.handlers = Some(handlers);
    }
}

impl Node {
    pub fn get_known_network(&self) -> Option<Vec<Node>> {
        self.known_network.clone()
    }

    /// Deeply compare each node and see if they are diferent, if they are diferent
    /// then return true, if they are equal then return false.
    pub fn nodes_are_different(&self, other: &Node) -> bool {
        self.name != other.name
            || self.key != other.key
            || self.status != other.status
            || self.description != other.description
            || self.version != other.version
            || self.handlers_differ(&other.handlers)
            || self.network_know_differ(&other.known_network)
    }

    /// This function was created to simplify the node comparation by deeply compare the nodes
    /// this comparator function will return true if the handlers are diferent and false if not
    fn handlers_differ(&self, other: &Option<Vec<NodeHandler>>) -> bool {
        match (&self.handlers, other) {
            (Some(a), Some(b)) => a.len() != b.len() || a.iter().zip(b.iter()).any(|(x, y)| x != y),
            (None, None) => false,
            _ => true,
        }
    }

    /// Allows to compare one network know with another, if the network known by the node is diferent than
    /// what it should be then it will say that the networks are diferent by a true value, if they are equal
    /// the value will be false because they aren't diferent.
    pub fn network_know_differ(&self, other: &Option<Vec<Node>>) -> bool {
        match (&self.known_network, other) {
            (Some(a), Some(b)) => a.len() != b.len() || a.iter().zip(b.iter()).any(|(x, y)| x.nodes_are_different(y)),
            (None, None) => false,
            _ => true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkMap {
    pub nodes: Vec<Node>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NetworkMapError {
    NodeDoNotExists(String),
    IncorrectValueMapPattern(String),
    IncorrectValuePattern,
    NodeNotInitialized(String),
}

impl From<NetworkMapError> for ChangeStatusError {
    fn from(error: NetworkMapError) -> Self {
        match error {
            NetworkMapError::NodeDoNotExists(s) => ChangeStatusError::NodeDoNotExists(s),
            NetworkMapError::IncorrectValueMapPattern(s) => ChangeStatusError::IncorrectValueMapPattern(s),
            NetworkMapError::IncorrectValuePattern => ChangeStatusError::IncorrectValuePattern,
            NetworkMapError::NodeNotInitialized(s) => ChangeStatusError::NodeNotInitialized(s),
        }
    }
}

impl NetworkMap {
    pub fn new(nodes: Vec<Node>) -> Self {
        Self { nodes }
    }

    /// Extracts all commands available for each handler with no filter
    ///
    /// This will return a Result with HashMap<String, Value> or a Error,
    /// the HashMap contains all the command available inside the msycelium
    /// network, all reachable and registred commands
    pub fn extract_all_commands(&self) -> Result<HashMap<String, IndexMap<String, String>>, NetworkMapError> {
        let mut available_commands: HashMap<String, IndexMap<String, String>> = HashMap::new();

        for node in &self.nodes {
            if let Some(handlers) = node.handlers.clone() {
                for handler in handlers {
                    available_commands.insert(handler.name.clone(), handler.parameters.clone());
                }
            } else {
                return Err(NetworkMapError::NodeNotInitialized("".to_string()));
            }
        }

        Ok(available_commands)
    }

    /// Extracts all the nodes filtering a specific node
    ///
    /// This allows to get all the nodes besides one node, wah tis a powerfull
    /// tool to the cases that need to get only the nodes that matters, for check some state
    /// like in the cases that only one node changes somethings and whe need to get all nodes besides
    /// this node in specific.
    pub fn get_all_nodes_except_node_with_key(&self, key: &String) -> Vec<Node> {
        let mut nodes_mirror = self.nodes.clone();
        if let Some(index) = nodes_mirror.iter().position(|x| x.key == Some(key.clone())) {
            nodes_mirror.remove(index); // Remove the specific node by key
        }
        nodes_mirror
    }

    pub fn change_nodes_status_except_node_with_key(&mut self, key: &String, new_status: NodeStatus) {
        let nodes = &mut self.get_all_nodes_except_node_with_key(key);
        for node in nodes {
            node.change_node_status(new_status.clone());
        }
    }

    pub fn get_all_nodes_except_node_with_name(&self, name: String) -> Vec<Node> {
        let mut nodes_mirror = self.nodes.clone();
        if let Some(index) = nodes_mirror.iter().position(|x| x.name == Some(name.clone())) {
            nodes_mirror.remove(index); // remove especific node
        }
        nodes_mirror
    }

    pub fn get_node_keys(&self) -> Result<HashMap<String, String>, NetworkMapError> {
        let mut valid_keys = HashMap::new();
        for node in &self.nodes {
            if let Some(node_name) = node.name.clone() {
                if let Some(node_key) = node.key.clone() {
                    valid_keys.insert(node_name.clone(), node_key.clone());
                } else {
                    return Err(NetworkMapError::NodeNotInitialized(node_name));
                }
            } else {
                return Err(NetworkMapError::NodeNotInitialized("".to_string()));
            }
        }
        return Ok(valid_keys);
    }

    pub fn get_node_by_name(&mut self, name: String) -> Result<&mut Node, NetworkMapError> {
        for node in &mut self.nodes {
            if node.name == Some(name.clone()) {
                return Ok(node);
            }
        }
        Err(NetworkMapError::NodeDoNotExists(name))
    }

    pub fn get_node_by_key(&mut self, key: &String) -> Result<&mut Node, NetworkMapError> {
        for node in &mut self.nodes {
            if node.key == Some(key.clone()) {
                return Ok(node);
            }
        }
        Err(NetworkMapError::NodeDoNotExists(key.clone()))
    }

    pub fn convert_to_value_map(&self) -> HashMap<String, Value> {
        let mut value_map = HashMap::new();
        value_map.insert("network_map".to_string(), serde_json::to_value(&self).unwrap());
        value_map
    }

    pub fn extract_to_value(&self) -> serde_json::Value {
        serde_json::to_value(&self).unwrap()
    }

    fn decode_value(value_object: Value) -> Result<NetworkMap, NetworkMapError> {
        let new_network_map: NetworkMap = match serde_json::from_value(value_object) {
            Ok(n) => n,
            Err(e) => {
                println!("Error creating network map from value: {:?}", e);
                return Err(NetworkMapError::IncorrectValuePattern);
            },
        };

        Ok(new_network_map)
    }

    pub fn update_from_value(&mut self, value_object: Value) -> Result<(), NetworkMapError> {
        let new_network_map: NetworkMap = NetworkMap::decode_value(value_object)?;
        self.mass_update_all_nodes(&new_network_map.nodes)?;
        Ok(())
    }

    pub fn gen_from_value(value_object: Value) -> Result<Self, NetworkMapError> {
        Ok(NetworkMap::decode_value(value_object)?)
    }

    pub fn update_from_value_map(&mut self, map: HashMap<String, Value>) -> Result<(), NetworkMapError> {
        if !map.contains_key("network_nodes") {
            return Err(NetworkMapError::IncorrectValueMapPattern("network map key not found in the map provided".to_string()));
        };

        let value_network_map = &map["network_nodes"];

        let network_map: Vec<Node> = match serde_json::from_value(value_network_map.clone()) {
            Ok(n) => n,
            Err(e) => return Err(NetworkMapError::IncorrectValueMapPattern(e.to_string())),
        };

        self.mass_update_all_nodes(&network_map).unwrap();

        Ok(())
    }

    pub fn target_is_reachable(&mut self, node_key: &String) -> Result<bool, NetworkMapError> {
        let _ = &self.get_node_by_key(node_key)?;
        return Ok(true); //> if node isn't in the network the the error will be returned above
    }

    pub fn target_is_ready(&mut self, node_key: &String) -> Result<bool, NetworkMapError> {
        let node = self.get_node_by_key(node_key)?;

        if let Some(status) = node.status.clone() {
            match status {
                NodeStatus::NotImplemented => {
                    return Ok(false);
                },
                NodeStatus::Online => {
                    return Ok(true);
                },
                NodeStatus::Offline => {
                    return Ok(false);
                },
                NodeStatus::NotSyncYet => {
                    return Ok(false);
                },
                NodeStatus::Idle => {
                    return Ok(true); // This represent the cases that node is restarting
                },
            }
            // TODO >> Maybe create a new case where the status can be InShutdown
        } else {
            return Ok(false);
        };
    }

    /// The idea of the update NetworkMap are to update the network
    /// by passing a Vec<Node> a vec of nodes, this allows to iterate in the
    /// current network map and update nodes based in the nodes contained in
    /// the vec of updated nodes, if a node exists then it will be updated
    /// with the values or the variables contained in this vec.
    pub fn mass_update_all_nodes(&mut self, updated_nodes: &Vec<Node>) -> Result<(), NetworkMapError> {
        // TODO >>> Add a better mechanism that can see if a node or function isn't implemented anymore in relation to the previous expectation

        let nnl = updated_nodes.len();
        let mut not_seen_nodes: Vec<String> = Vec::new();

        let registred_node_keys: Vec<String> = self.get_node_keys()?.values().cloned().collect();
        let mut not_implemented_nodes: Vec<String> = Vec::new();

        let mut new_nodes: HashMap<String, Node> = HashMap::new();
        let mut new_nodes_keys: Vec<String> = Vec::new();

        for nn in updated_nodes {
            if let Some(nn_key) = nn.key.clone() {
                new_nodes.insert(nn_key.clone(), nn.clone());
                new_nodes_keys.push(nn_key.clone());
            }
        }

        not_seen_nodes = new_nodes_keys.clone();

        // -> UPDATE EXISTING NODES:

        for node in &mut self.nodes {
            if let Some(node_key) = &node.key.clone() {
                if new_nodes_keys.contains(node_key) {
                    //> UPDATE NODES THAT STILL EXISTING
                    let new_node = &new_nodes[node_key];
                    *node = new_node.clone();

                    if let Some(index) = not_seen_nodes.iter().position(|x| x == node_key) {
                        not_seen_nodes.remove(index); // remove seen nodes
                    }
                } else {
                    //> UPDATE NODES THAT DON'T EXISTS ANYMORE
                    node.status = Some(NodeStatus::NotImplemented);
                };
            }
        }

        // -> CREATE NEW NODES:

        for key in not_seen_nodes {
            let new_node = new_nodes[&key].clone();

            self.nodes.push(Node::partially_initialize(
                new_node.name.unwrap(),
                new_node.key.unwrap(),
                new_node.status.unwrap(),
                new_node.description,
                new_node.version,
                new_node.handlers,
            ))
        }

        return Ok(());
    }

    pub fn handler_exists_in(&mut self, owner: &str, command_name: &str) -> bool {
        let node = match self.get_node_by_key(&owner.to_string()) {
            Ok(n) => n,
            Err(_) => {
                return false;
            },
        };

        if let Some(node_handlers) = &node.handlers {
            for handler in node_handlers {
                if handler.name == command_name {
                    return true;
                }
            }
        }

        return false;
    }

    pub fn add_or_update_if_exists(&mut self, new_node: Node) {
        // -> UPDATE EXISTING NODE:
        for node in &mut self.nodes {
            if new_node.key == node.key {
                // *node = new_node;

                node.name = new_node.name;
                node.status = new_node.status;
                node.description = new_node.description;
                node.version = new_node.version;
                node.handlers = new_node.handlers;

                // known_network: Option<Vec<Node>>,

                return;
            } else {
                continue;
            }
        }

        // -> CREATE NEW NODE:
        self.nodes.push(new_node)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Command {
    parameters: HashMap<String, String>,
    status: CommandStatus,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum CommandStatus {
    Active,
    Inactive,
    // Add more status types as needed
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CommandPatterns {
    //> Structure:

    //> "owner": {
    //>     "command_name": Comamnd {
    //>         "parameters": HashMap<String, String>,
    //>         "status": CommandStatus,
    //>     }
    //> }

    // -> Wrap patterns
    patterns: HashMap<String, HashMap<String, Command>>,
}

impl CommandPatterns {
    pub fn new() -> Self {
        CommandPatterns { patterns: HashMap::new() }
    }

    pub fn command_exists(&self, owner: &str, command_name: &str) -> bool {
        self.patterns.get(owner).and_then(|commands| commands.get(command_name)).is_some()
    }

    pub fn add_command(&mut self, owner: String, command_name: String, command: Command) {
        self.patterns.entry(owner).or_insert_with(HashMap::new).insert(command_name, command);
    }

    // Function to parse the JSON and integrate it into CommandPatterns
    pub fn add_json(&mut self, owner: &str, json_str: &str) -> Result<(), serde_json::Error> {
        // Parse the JSON string
        let parsed: HashMap<String, Value> = serde_json::from_str(json_str)?;

        // Iterate over the parsed data and integrate it into CommandPatterns
        for (command_name, params) in parsed {
            let mut command_params = HashMap::new();

            match params {
                Value::Object(obj) => {
                    for (param_name, param_type) in obj {
                        if let Value::String(type_str) = param_type {
                            command_params.insert(param_name, type_str);
                        }
                    }
                },
                _ => (), // Handle other types like Array, if necessary
            }

            let command = Command {
                parameters: command_params,
                status: CommandStatus::Active, // Assuming default status as Active
            };

            self.add_command(owner.to_string(), command_name, command);
        }

        Ok(())
    }

    // Function to integrate a HashMap<String, Value> as commands for a client
    pub fn add_commands_from_map(&mut self, client: &str, commands_map: HashMap<String, Value>) {
        let client_commands = self.patterns.entry(client.to_string()).or_insert_with(HashMap::new);

        for (command_name, params) in commands_map {
            let mut command_params = HashMap::new();

            match params {
                Value::Object(obj) => {
                    for (param_name, param_type) in obj {
                        if let Value::String(type_str) = param_type {
                            command_params.insert(param_name, type_str);
                        }
                        // Handle other Value types if necessary
                    }
                },
                _ => (), // Handle non-Object types if necessary
            }

            let command = Command {
                parameters: command_params,
                status: CommandStatus::Active, // Assuming default status as Active
            };

            // Add or update the command
            client_commands.insert(command_name, command);
        }
    }

    // Function to add or update commands for a client
    pub fn add_or_update_if_exists(&mut self, client: &str, commands_map: HashMap<String, Value>) {
        if self.patterns.contains_key(client) {
            // If the client already exists, update its commands
            // You might need additional logic here to properly merge or update the commands
            self.add_commands_from_map(client, commands_map);
        } else {
            // If the client does not exist, add it
            self.patterns.insert(client.to_string(), HashMap::new());
            self.add_commands_from_map(client, commands_map);
        }
    }

    pub fn extract_command_params_for_client(&self, client: &str, command_name: &str) -> Option<HashMap<String, Value>> {
        // Attempt to retrieve the command for the specified client
        if let Some(client_commands) = &self.patterns.get(client) {
            if let Some(command) = client_commands.get(command_name) {
                let mut params_map = HashMap::new();

                // Iterate over the command parameters and convert them to Value
                for (param_name, param_type) in &command.parameters {
                    params_map.insert(param_name.clone(), Value::String(param_type.clone()));
                }

                return Some(params_map);
            }
        }
        None
    }

    // Function to extract a HashMap of all clients, each with their own commands and parameters
    pub fn extract_all_commands(&self) -> HashMap<String, Value> {
        let mut all_clients_commands = HashMap::new();

        // Iterate over all clients
        for (client_name, client_commands) in &self.patterns {
            let mut client_commands_map = serde_json::Map::new();

            // Iterate over each command for the client
            for (command_name, command) in client_commands {
                let params_value = command.parameters.iter().map(|(k, v)| (k.clone(), Value::String(v.clone()))).collect::<serde_json::Map<_, _>>();

                client_commands_map.insert(command_name.clone(), Value::Object(params_value));
            }

            all_clients_commands.insert(client_name.clone(), Value::Object(client_commands_map));
        }

        all_clients_commands
    }

    // Function to get all commands except for those of a specified client, formatted as a HashMap<String, Value>
    pub fn get_all_commands_except_for_client(&self, excluded_client: &str) -> HashMap<String, Value> {
        let mut filtered_commands = HashMap::new();

        for (client_name, client_commands) in &self.patterns {
            if client_name != excluded_client {
                let mut client_commands_map = Map::new();

                // Iterate over each command for the client
                for (command_name, command) in client_commands {
                    let params_value = command.parameters.iter().map(|(k, v)| (k.clone(), Value::String(v.clone()))).collect::<Map<_, _>>();

                    client_commands_map.insert(command_name.clone(), Value::Object(params_value));
                }

                filtered_commands.insert(client_name.clone(), Value::Object(client_commands_map));
            }
        }

        filtered_commands
    }

    pub fn remove_command(&mut self, owner: &str, command_name: &str) {
        if let Some(commands) = self.patterns.get_mut(owner) {
            commands.remove(command_name);
        }
    }

    // Function to create a new CommandPatterns struct from a HashMap<String, Value>
    pub fn create_command_patterns_from_map(commands_map: HashMap<String, Value>) -> Self {
        let mut command_patterns = Self::new();

        // Iterate over the outer map, where each key is a client name
        for (client_name, client_commands_value) in commands_map {
            if let Value::Object(client_commands_map) = client_commands_value {
                // Iterate over each command for the client
                for (command_name, params_value) in client_commands_map {
                    if let Value::Object(params_map) = params_value {
                        let command_params = params_map
                            .into_iter()
                            .filter_map(|(k, v)| {
                                if let Value::String(value_str) = v {
                                    Some((k, value_str))
                                } else {
                                    None // Filter out non-string values
                                }
                            })
                            .collect::<HashMap<String, String>>();

                        let command = Command {
                            parameters: command_params,
                            status: CommandStatus::Active, // Default status, can be adjusted as needed
                        };

                        command_patterns.add_command(client_name.clone(), command_name, command);
                    }
                }
            }
        }

        command_patterns
    }

    // Function to create a new CommandPatterns struct from a HashMap<String, Value>
    pub fn add_from_map(&mut self, commands_map: HashMap<String, Value>) -> Self {
        // Iterate over the outer map, where each key is a client name
        for (client_name, client_commands_value) in commands_map {
            if let Value::Object(client_commands_map) = client_commands_value {
                // Iterate over each command for the client
                for (command_name, params_value) in client_commands_map {
                    if let Value::Object(params_map) = params_value {
                        let command_params = params_map
                            .into_iter()
                            .filter_map(|(k, v)| {
                                if let Value::String(value_str) = v {
                                    Some((k, value_str))
                                } else {
                                    None // Filter out non-string values
                                }
                            })
                            .collect::<HashMap<String, String>>();

                        let command = Command {
                            parameters: command_params,
                            status: CommandStatus::Active, // Default status, can be adjusted as needed
                        };

                        self.add_command(client_name.clone(), command_name, command);
                    }
                }
            }
        }

        self.clone()
    }
}

/*
-> Example of usage:
``` Rust
lazy_static! {
    static ref GLOBAL_COMMAND_PATTERNS: Mutex<CommandPatterns> = Mutex::new(CommandPatterns::new());
}

fn main() {
    let mut command_patterns = GLOBAL_COMMAND_PATTERNS.lock().unwrap();

    Define and add 'get_symbols_data' command to 'client1'
    let get_symbols_data_params = create_command_params(&[
        ("data-type", "str"),
        ("symbols", "str"),
        ("start-ts", "float"),
        ("end-ts", "float"),
        ]);
        let get_symbols_data_command = Command {
            parameters: get_symbols_data_params,
            status: CommandStatus::Active,
        };
        command_patterns.add_command("client1".into(), "get_symbols_data".into(), get_symbols_data_command);

        Similarly, define and add 'get_other_symbols_data' command
        let get_other_symbols_data_params = create_command_params(&[
            ("data-type", "str"),
            ("symbols", "str"),
            ("start-ts", "float"),
            ("end-ts", "float"),
            ]);
            let get_other_symbols_data_command = Command {
                parameters: get_other_symbols_data_params,
                status: CommandStatus::Active,
            };
            command_patterns.add_command("client1".into(), "get_other_symbols_data".into(), get_other_symbols_data_command);

            // Now, 'command_patterns' contains the two commands for 'client1'
        }

```
*/
