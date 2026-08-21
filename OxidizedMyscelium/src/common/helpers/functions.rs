// SPDX-License-Identifier: MPL-2.0
// Copyright © 2021-2026 Cristian Camargo Filho

use std::fs;
use std::path::Path;

pub fn remove_directory(path: &str) -> std::io::Result<()> {
    let dir_path = Path::new(path);
    if dir_path.exists() && dir_path.is_dir() {
        fs::remove_dir_all(dir_path)?;
        println!("Successfully removed the directory and its contents.");
    } else {
        println!("The directory does not exist or is not a directory.");
    }
    Ok(())
}
