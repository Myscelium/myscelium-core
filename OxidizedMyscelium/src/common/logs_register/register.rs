// SPDX-License-Identifier: MPL-2.0
// Copyright © 2021-2026 Cristian Camargo Filho

use lazy_static::lazy_static;
use std::fs::OpenOptions;
use std::{fs::File, sync::Arc};
use tokio::sync::Mutex;

use std::io::Write;
use std::io::{self, Error};

use std::fs;
use std::path::{Path, PathBuf};

// use crate::common::functions::advanced_lockers::smart_lock;

lazy_static! {
    static ref FILE: Arc<Mutex<Option<File>>> = Arc::new(Mutex::new(None));
}

pub async fn initialize_logs_file(path: &str) -> io::Result<()> {
    let mut file_path = PathBuf::from(path);

    // if it exists and is really a directory → true
    // if it doesn’t exist → assume it’s a directory you plan to create
    // any other error we just bubble up
    let is_dir = match fs::metadata(&file_path) {
        Ok(m) => m.is_dir(),
        Err(ref e) if e.kind() == io::ErrorKind::NotFound => true,
        Err(e) => return Err(e),
    };

    if is_dir {
        file_path.push("logs.txt");
    }

    // now make sure parent exists…
    let parent = file_path.parent().unwrap(); // always Some, even for "logs.txt"
    fs::create_dir_all(parent)?;

    println!("Opening logfile at {:?}", file_path);
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&file_path)?;

    let mut guard = FILE.lock().await;
    *guard = Some(file);
    Ok(())
}

pub async fn write_to_file(text: String) -> io::Result<()> {
    let mut guard = FILE.lock().await;
    if let Some(f) = guard.as_mut() {
        let line = format!("{}\n", text);
        let mut file = f.try_clone()?; // owned handle for the blocking task
        tokio::task::spawn_blocking(move || {
            file.write_all(line.as_bytes())?;
            file.flush()
        })
        .await??; // await the JoinHandle, then the io::Result
    }
    Ok(())
}
