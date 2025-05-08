#[macro_export]
macro_rules! with_connection {
    ($pool:expr, $body:expr) => {
        (async {
            let pool_clone = $pool.clone();
            let mut pool_guard = pool_clone.lock().await;
            let conn = pool_guard.get_connection().await.expect("Failed to get connection");

            // move conn into the closure, and expect it to return it back
            let (result, conn) = $body(conn).await;

            let _ = pool_guard.release_connection(conn).await;
            result
        })
    };
}

#[macro_export]
macro_rules! set_new_path_to_buffer_db {
    ($pool:expr, $num_of_workers:expr, $buffer_path:expr, $buffer_name:expr) => {
        (async {
            use std::path::Path;
            use tokio::sync::Mutex;
            use tokio::task;

            let default_name_path = {
                let default_buffer_path = $buffer_name.lock().await;
                default_buffer_path.clone()
            };

            let new_buffer_path = format!("{}{}", $buffer_path, default_name_path);

            // Create the directory if it doesn't exist (blocking op in spawn_blocking)
            let buffer_path_clone = $buffer_path.to_string();
            task::spawn_blocking(move || {
                let dir_path = Path::new(&buffer_path_clone);
                if !dir_path.exists() {
                    std::fs::create_dir_all(&dir_path).expect("Failed to create buffer directory");
                }
            })
            .await
            .expect("Directory creation task panicked");

            println!("Initializing buffer in: {}", new_buffer_path);

            let num_workers_clone = {
                let num_workers = $num_of_workers.lock().await;
                *num_workers as usize
            };

            let new_pool = SQLiteConnectionPool::new(num_workers_clone, new_buffer_path.as_str()).await.unwrap();

            let mut buffer_pool = $pool.lock().await;
            *buffer_pool = new_pool;
        })
    };
}
