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

use crate::common::functions::advanced_lockers::smart_lock;

lazy_static! {
    static ref FILE: Arc<Mutex<Option<File>>> = Arc::new(Mutex::new(None));
}

pub fn initialize_buffer_history(file_path: &String) -> Result<(), Error> {
    // Attempt to open the file in append mode at the given file path

    let path = &format!("{}{}", file_path.clone(), "buffer_history.txt");

    println!("initializing buffer history in: {}", path);

    let file_result = OpenOptions::new().create(true).append(true).open(path);

    match file_result {
        Ok(file) => {
            // Use smart_lock to safely update FILE with the opened file
            smart_lock(&FILE, |file_option: &mut Option<File>| {
                *file_option = Some(file);
            });
            Ok(())
        }
        Err(e) => {
            // Handle errors (e.g., file not created, cannot open, etc.)
            eprintln!("Error occurred initializing the buffer history!");
            Err(e)
        }
    }
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
