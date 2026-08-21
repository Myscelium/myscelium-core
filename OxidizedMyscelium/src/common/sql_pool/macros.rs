// SPDX-License-Identifier: MPL-2.0
// Copyright © 2021-2026 Cristian Camargo Filho

#[macro_export]
macro_rules! with_connection {
    ($pool:expr, $body:expr) => {{
        let conn = $pool.lock().get_connection().unwrap();
        let result = $body(&conn);
        $pool.lock().release_connection(conn);
        result.clone()
    }};
}

#[macro_export]
macro_rules! set_new_path_to_buffer_db {
    ($pool:expr, $num_of_workers:expr, $buffer_path:expr, $buffer_name:expr) => {{
        let new_buffer_path;

        {
            let default_name_path;
            {
                let default_buffer_path = $buffer_name.lock();
                default_name_path = default_buffer_path.clone();
            }

            new_buffer_path = format!("{}{}", $buffer_path, default_name_path);

            // *default_buffer_path = new_buffer_path.clone();

            // Create the directory if it does not exist
            let dir_path = std::path::Path::new(&$buffer_path);
            if !dir_path.exists() {
                std::fs::create_dir_all(&dir_path).unwrap();
            }

            println!("initializing buffer in: {}", new_buffer_path);
        }

        {
            let num_workers_clone;

            {
                // -> this is a dependency of BUFFER_POOL so need to stay in other block like that to don't lock the thread
                let num_workers = $num_of_workers.lock();
                num_workers_clone = *num_workers as usize;
            }

            let new_pool = SQLiteConnectionPool::new(num_workers_clone, new_buffer_path.as_str()).unwrap();

            let mut buffer_pool = $pool.lock();
            *buffer_pool = new_pool;
        }
    }};
}
