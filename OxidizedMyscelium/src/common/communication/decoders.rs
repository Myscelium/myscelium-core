// SPDX-License-Identifier: MPL-2.0
// Copyright © 2021-2026 Cristian Camargo Filho

use crate::common::enhanced_buffer::utilities::Command;
use serde_json::Result as JsonResult;
use std::io::{ErrorKind, Read};
use std::net::TcpStream;

pub fn read_json_from_stream(
    stream: &mut TcpStream,
) -> Result<Command, Box<dyn std::error::Error>> {
    let mut buffer = Vec::new();
    let mut temp_buffer = [0; 1024]; // Smaller buffer for partial reads
    let mut received_something = false;
    loop {
        match stream.read(&mut temp_buffer) {
            Ok(0) => {
                if received_something {
                    break; // End of data
                } else {
                    continue;
                }
            }
            Ok(size) => {
                received_something = true;
                buffer.extend_from_slice(&temp_buffer[..size])
            }
            Err(e) => return Err(Box::new(e)), // Convert IoError to Box<dyn Error>
        }
    }

    let buffer_string = String::from_utf8_lossy(&buffer)
        .trim_end_matches(|c| c == '\n' || c == '\r' || c == '\0')
        .to_string();

    serde_json::from_str(&buffer_string).map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
}
