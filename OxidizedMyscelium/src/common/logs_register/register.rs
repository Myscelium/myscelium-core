// SPDX-License-Identifier: MPL-2.0
// Copyright © 2021-2026 Cristian Camargo Filho

use lazy_static::lazy_static;
use std::fs::OpenOptions;
use std::{
    fs::File,
    sync::{Arc, Mutex},
};

use std::io::Error;
use std::io::Write;

use std::fs;
use std::path::{Path, PathBuf};

use crate::common::functions::advanced_lockers::smart_lock;

lazy_static! {
    static ref FILE: Arc<Mutex<Option<File>>> = Arc::new(Mutex::new(None));
}

pub fn initialize_logs_file(file_path: &str) -> Result<(), Error> {
    let mut full_path = Path::new(file_path).to_path_buf();

    // Debug print to understand the path structure
    println!("Received path: {:?}", full_path);

    if !full_path.ends_with("logs.txt") {
        // If the path does not end with 'logs.txt', we assume it's a directory and append the filename
        if !full_path.is_dir() {
            // Append a directory separator if not present
            full_path.push(""); // Pushing an empty string appends the default directory separator
        }
        full_path.push("logs.txt"); // Now append the filename
    }

    // At this point, full_path should end with 'logs.txt', and its parent is the directory we want to ensure exists
    if let Some(parent_dir) = full_path.parent() {
        // Debug print to see what directory we are trying to create
        println!("Attempting to create directory: {:?}", parent_dir);

        fs::create_dir_all(parent_dir)?;
    } else {
        return Err(Error::new(
            std::io::ErrorKind::NotFound,
            "No parent directory found.",
        ));
    }

    // Debug print the final file path
    println!("Final file path: {:?}", full_path);

    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&full_path)?;

    // Use smart_lock to safely update FILE with the opened file
    let mut file_option = FILE.lock().unwrap();
    *file_option = Some(file);

    Ok(())
}

pub fn write_to_file(text: String) {
    let file = &FILE;
    smart_lock(file, |file_option: &mut Option<File>| {
        if let Some(f) = file_option {
            writeln!(f, "{}", text).unwrap();
        } else {
            // Handle the case where the file is not initialized
            println!("File is not initialized.");
        }
    });
}
