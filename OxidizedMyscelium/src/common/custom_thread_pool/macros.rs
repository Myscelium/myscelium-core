// SPDX-License-Identifier: MPL-2.0
// Copyright © 2021-2026 Cristian Camargo Filho

#[macro_export]
macro_rules! init_thread_pool {
    ($size:expr) => {{
        use std::sync::{mpsc, Arc, Mutex};
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let pool = Arc::new(Mutex::new(
                crate::common::custom_thread_pool::thread_pool::UnifiedThreadPool::new($size),
            ));
            if let Err(err) = tx.send(pool) {
                println!("Error initializing thread pool: {:?}", err);
            }
        });
        match rx.recv() {
            Ok(pool) => pool,
            Err(err) => {
                println!("Error receiving thread pool: {:?}", err);
                panic!("Failed to initialize thread pool!"); // or handle the error as appropriate
            }
        }
    }};
}

#[macro_export]
macro_rules! terminate_pool {
    ($pool:expr) => {{
        let mut locked_pool = $pool.lock().unwrap();
        locked_pool.stop();
    }};
}

#[macro_export]
macro_rules! run_in_thread_pool {
    ($pool:expr, $code:block) => {{
        use std::sync::mpsc;
        let (tx, rx) = mpsc::channel();
        let mut locked_pool = $pool.lock().unwrap();
        locked_pool.execute(move || {
            let result = $code;
            if let Err(err) = tx.send(result) {
                println!("Error sending result from thread: {:?}", err);
            }
        });
        rx
    }};
}

#[macro_export]
macro_rules! wait_all_threads {
    ($receivers:expr) => {{
        let mut results = Vec::new();
        for rx in $receivers {
            match rx.recv() {
                Ok(result) => results.push(result),
                Err(err) => {
                    println!("Error receiving result from thread: {:?}", err);
                }
            }
        }
        results
    }};
}
