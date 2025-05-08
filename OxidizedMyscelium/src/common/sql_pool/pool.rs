extern crate rand;
use rand::distr::Alphanumeric;
use rand::Rng;

use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

use lazy_static::lazy_static;

use rusqlite::{Connection, Result};
use std::result::Result::Err;
use std::result::Result::Ok;

// Similarly `mod inaccessible` and `mod nested` will locate the `nested.rs`
// and `inaccessible.rs` files and insert them here under their respective
// modules

lazy_static! {
    static ref ID_LENGTH: Mutex<i32> = Mutex::new(9999);
}

/*
   However, the rusqlite library in Rust automatically starts a new
   transaction before each command and commits it after the command
   is executed, unless you explicitly start a transaction. This is
   known as "autocommit mode".

*/

/// A utility structure for generating unique parity IDs as strings.
/// The uniqueness of the ID is ensured within the context of previously generated IDs
/// which are stored in the `registered_ids` field.
pub struct UniqueParityIdGenerator {
    /// Length of the ID string to be generated.
    length: usize,
    /// A list of previously generated IDs to ensure the uniqueness of new IDs.
    registered_ids: Vec<String>,
}

/// A utility structure for generating unique IDs as integers.
/// The uniqueness of the ID is ensured within the context of previously generated IDs
/// which are stored in the `registered_ids` field.
// pub struct UniqueIdGenerator {
//     /// A list of previously generated IDs to ensure the uniqueness of new IDs.
//     pub registered_ids: Vec<u32>,
// }

/// A utility for generating unique parity IDs.
impl UniqueParityIdGenerator {
    /// Constructs a new generator.
    ///
    /// # Parameters
    /// - `length`: Length of the generated ID.
    /// - `registered_ids`: A list of already registered IDs to avoid collisions.
    ///
    /// # Returns
    /// An instance of `UniqueParityIdGenerator`.
    pub fn new(length: usize, registered_ids: Vec<String>) -> Self {
        Self { length, registered_ids }
    }

    /// Updates the internal list of registered IDs.
    ///
    /// # Parameters
    /// - `registered_ids`: The new list of registered IDs.
    pub fn update_registered_parity_ids(&mut self, registered_ids: Vec<String>) {
        self.registered_ids = registered_ids;
    }

    /// Generates a new unique parity ID.
    ///
    /// # Returns
    /// A `String` representing the unique parity ID.
    pub fn gen(&mut self) -> String {
        loop {
            let buffer_id = self.random_string();
            if self.validate(&buffer_id) {
                return buffer_id;
            }
        }
    }

    /// Generates a random string of the specified length.
    ///
    /// # Returns
    /// A `String` of random alphanumeric characters.
    fn random_string(&self) -> String {
        let rng = rand::thread_rng();
        let id: String = rng.sample_iter(&Alphanumeric).take(self.length).map(char::from).collect();
        id
    }

    /// Validates that a given ID is unique.
    ///
    /// # Parameters
    /// - `buffer_id`: The ID to validate.
    ///
    /// # Returns
    /// `true` if the ID is unique, otherwise `false`.
    fn validate(&self, buffer_id: &String) -> bool {
        !self.registered_ids.contains(buffer_id)
    }
}

/// A utility for generating unique numeric IDs.
// impl UniqueIdGenerator {
//     /// Constructs a new generator.
//     ///
//     /// # Parameters
//     /// - `registered_ids`: A list of already registered IDs to avoid collisions.
//     ///
//     /// # Returns
//     /// An instance of `UniqueIdGenerator`.
//     pub fn _new(registered_ids: Vec<u32>) -> Self {
//         Self { registered_ids }
//     }

//     /// Updates the internal list of registered IDs.
//     ///
//     /// # Parameters
//     /// - `registered_ids`: The new list of registered IDs.
//     pub fn _update_registered_ids(&mut self, registered_ids: Vec<u32>) {
//         self.registered_ids = registered_ids;
//     }

//     /// Generates a new unique ID.
//     ///
//     /// # Returns
//     /// A `u32` representing the unique ID.
//     pub fn gen(&mut self) -> u32 {
//         loop {
//             let buffer_id = self.gen_buffer_id();
//             if self.validate(buffer_id) {
//                 return buffer_id;
//             }
//         }
//     }

//     /// Generates a random numeric ID.
//     ///
//     /// # Returns
//     /// A `u32` representing the randomly generated ID.
//     fn gen_buffer_id(&self) -> u32 {
//         let length = ID_LENGTH.lock();
//         let mut rng = rand::thread_rng();
//         rng.gen_range(0..*length) as u32
//     }

//     /// Validates that a given numeric ID is unique.
//     ///
//     /// # Parameters
//     /// - `buffer_id`: The ID to validate.
//     ///
//     /// # Returns
//     /// `true` if the ID is unique, otherwise `false`.
//     fn validate(&self, buffer_id: u32) -> bool {
//         !self.registered_ids.contains(&buffer_id)
//     }
// }

// -> Sql custom pool:
use std::fmt;

/// Represents errors that can occur when dealing with the SQLite connection pool.
#[derive(Debug)]
pub enum PoolError {
    /// An error specific to SQLite operations.
    SqliteError(rusqlite::Error),
    /// An error related to sending data over a channel.
    SendError(String),
    /// An error indicating that there are no available connections in the pool.
    NoAvailableConnections,
}

impl fmt::Display for PoolError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PoolError::SqliteError(e) => write!(f, "SQLite error: {}", e),
            PoolError::SendError(e) => write!(f, "Send error: {}", e),
            PoolError::NoAvailableConnections => write!(f, "No available connections in the pool"),
        }
    }
}

impl std::error::Error for PoolError {}

impl From<rusqlite::Error> for PoolError {
    fn from(err: rusqlite::Error) -> PoolError {
        PoolError::SqliteError(err)
    }
}

pub struct SQLiteConnectionPool {
    /// A channel receiver for getting connections from the pool.
    connections: Arc<Mutex<mpsc::Receiver<Connection>>>,
    /// A channel sender for returning connections to the pool.
    sender: mpsc::Sender<Connection>,
}

impl SQLiteConnectionPool {
    /// Creates a new SQLite connection pool with a given maximum number of connections to a specified database.
    ///
    /// # Parameters
    /// - `max_connections`: The maximum number of connections this pool should maintain.
    /// - `db`: The path to the SQLite database.
    ///
    /// # Returns
    /// A `Result` which is:
    /// - `Ok`: Contains the initialized `SQLiteConnectionPool`.
    /// - `Err`: Contains a `PoolError` indicating the error that occurred.
    pub async fn new(max_connections: usize, db: &str) -> Result<Self, PoolError> {
        let (tx, rx) = mpsc::channel(max_connections);
        for _ in 0..max_connections {
            let conn = Connection::open(db)?;
            tx.send(conn).await.map_err(|e| PoolError::SendError(e.to_string()))?;
        }
        Ok(Self {
            connections: Arc::new(Mutex::new(rx)),
            sender: tx,
        })
    }

    /// Creates an empty SQLite connection pool.
    /// This is mainly useful for scenarios where the pool might be populated later or under certain conditions.
    ///
    /// # Returns
    /// An instance of `SQLiteConnectionPool` with no active connections.
    pub fn empty() -> Self {
        let (tx, rx) = mpsc::channel(32);

        Self {
            connections: Arc::new(Mutex::new(rx)),
            sender: tx,
        }
    }

    /// Attempts to retrieve a connection from the pool.
    ///
    /// # Returns
    /// A `Result` which is:
    /// - `Ok`: Contains a `Connection` object if one is available.
    /// - `Err`: Contains a `PoolError` indicating that no connections are available.
    pub async fn get_connection(&self) -> Result<Connection, PoolError> {
        let mut rx = self.connections.lock().await;
        match rx.recv().await {
            Some(conn) => Ok(conn),
            None => Err(PoolError::NoAvailableConnections),
        }
    }

    /// Returns the provided connection back to the pool, making it available for reuse.
    ///
    /// # Parameters
    /// - `connection`: The `Connection` object to be returned to the pool.
    pub async fn release_connection(&self, connection: Connection) {
        let lock = &self.sender;
        lock.send(connection).await;
    }
}

// In this rust code above:

//-> SQLiteConnectionPool:
//> is a struct with two fields: max_connections and connections.
//> Connections is a receiver end of a channel, wrapped in an
//> Arc<Mutex<_>> fro safety.

//-> new:
//> is a constructor that creates a new instance of SQLiteConnectionPool. it opens max_connections
//> number of SQLite connections and sends them into the channel.

//-> get_connection:
//> Tries to receive a connection from the channel. if the channel is empty (i.e, all connections are in use),
//> it returns an error.

//-> release_connection:
//> Sends a connection back into the cannel

/*

Also, please note that error handling in Rust is different from Python.
In Rust, you typically return a Result from the functons that can fail, and the
caller is responsible for handling the error. In this code, 'get_connection'
returns a Result<Connection>, wich means it returns either a Connection or an
Error. If the channel is empty, it returns an error.

Finaly, please note that Rust uses snake_case for function and variable names, not
camelCase or PascalCase. This is a convention in the Rust community annd is enforced by the
compiler's built in linter, 'rustc'

*/
