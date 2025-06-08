use lazy_static::lazy_static;
use std::fs::OpenOptions;
use std::{fs::File, sync::Arc};
use tokio::sync::Mutex;

use std::io::Error;
use std::io::Write;
use tokio::io;
use tokio::task;

// use crate::common::functions::advanced_lockers::smart_lock;

lazy_static! {
    static ref FILE: Arc<tokio::sync::Mutex<Option<File>>> = Arc::new(tokio::sync::Mutex::new(None));
}

pub async fn initialize_buffer_history(file_path: &String) -> Result<(), Error> {
    // Attempt to open the file in append mode at the given file path

    let path = &format!("{}{}", file_path.clone(), "buffer_history.txt");

    println!("initializing buffer history in: {}", path);

    let file_result = OpenOptions::new().create(true).append(true).open(path);

    match file_result {
        Ok(file) => {
            let mut file_option = FILE.lock().await;
            *file_option = Some(file);

            // Use smart_lock to safely update FILE with the opened file
            // smart_lock(&FILE, |file_option: &mut Option<File>| {
            //     *file_option = Some(file);
            // });
            Ok(())
        },
        Err(e) => {
            // Handle errors (e.g., file not created, cannot open, etc.)
            eprintln!("Error occurred initializing the buffer history!");
            Err(e)
        },
    }
}

pub async fn write_to_file(text: String) -> io::Result<()> {
    let mut guard = FILE.lock().await; // FILE: Mutex<Option<std::fs::File>>
    if let Some(f) = guard.as_mut() {
        let line = format!("{}\n", text);
        let mut file = f.try_clone()?; // std::fs::File — blocking handle

        // Off‑load the blocking write onto the thread‑pool reserved for it
        task::spawn_blocking(move || {
            // tokio::task::spawn_blocking
            file.write_all(line.as_bytes())?; // std::io::Write::write_all
            file.flush() // std::io::Write::flush
        })
        .await??; // wait for the background thread
    } else {
        eprintln!("[LOGGING] skipped write because FILE is not initialized: {text}");
    }
    Ok(())
}

// smart_lock(file, |file_option: &mut Option<File>| {
//     if let Some(f) = file_option {
//         writeln!(f, "{}", text).unwrap();
//     } else {
//         // Handle the case where the file is not initialized
//         println!("File is not initialized.");
//     }
// });
