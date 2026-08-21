// SPDX-License-Identifier: MPL-2.0
// Copyright © 2021-2026 Cristian Camargo Filho

use crate::common::structs::available_commands::{Command, CommandPatterns};

use serde_json::Value;
use std::collections::HashMap;

// Utility function to create command parameters
fn create_command_params(params: &[(&str, &str)]) -> HashMap<String, String> {
    params
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}
