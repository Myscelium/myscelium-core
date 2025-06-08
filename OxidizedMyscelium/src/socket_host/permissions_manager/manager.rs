use lazy_static::lazy_static;

use crate::common::sql_pool::pool::{SQLiteConnectionPool, UniqueParityIdGenerator};
use crate::{set_new_path_to_buffer_db, with_connection};

use rusqlite::params;

use std::sync::Arc;
use tokio::sync::Mutex;

use serde_json::{from_str, Value};

use rusqlite::{Connection, Result};

use crate::common::client_manager::manager::{Client, ClientError};

use rusqlite::Row;
use rusqlite::Statement;
use std::thread;
use std::time::Duration;

lazy_static! {
    static ref BUFFER_NAME: Arc<Mutex<String>> = Arc::new(Mutex::new("buffer.db".to_string()));
    static ref BUFFER_PATH: Arc<Mutex<String>> = Arc::new(Mutex::new("buffer.db".to_string()));
    static ref NUM_WORKERS: Arc<Mutex<u32>> = Arc::new(Mutex::new(5));
    static ref SQL_POOL: Arc<Mutex<SQLiteConnectionPool>> = Arc::new(Mutex::new(SQLiteConnectionPool::empty()));
}

pub async fn set_workers_num(n_workers: u32) {
    let mut default_num_of_workers = NUM_WORKERS.lock().await;
    *default_num_of_workers = n_workers;
}

pub async fn groups_mananger_initialize_table(buffer_path: String) {
    // Create a global Mutex for demonstration
    let mutex1 = Mutex::new(0);
    let mutex2 = Mutex::new(0);

    // Spawn a thread to periodically check for deadlocks
    thread::spawn(|| {
        loop {
            thread::sleep(Duration::from_secs(5)); // Check every 5 seconds
            let deadlocks = parking_lot::deadlock::check_deadlock();
            if deadlocks.is_empty() {
                continue;
            }

            println!("{} deadlocks detected", deadlocks.len());
            for (i, threads) in deadlocks.iter().enumerate() {
                println!("Deadlock #{}", i);
                for t in threads {
                    println!("Thread Id {:?}", t.thread_id());
                    println!("{:?}", t.backtrace());
                }
            }
        }
    });

    set_new_path_to_buffer_db!(SQL_POOL, NUM_WORKERS, buffer_path, BUFFER_NAME).await;

    with_connection!(SQL_POOL, |conn: rusqlite::Connection| async {
        let result = conn.execute("CREATE TABLE IF NOT EXISTS PermissionGroups (ID INT PRIMARY KEY AUTOINCREMENT, GroupName TEXT, AllowedCallbacks TEXT, AllowCreateNewClients BOOL, AllowCreateSubChannels BOOL, MaxSubChannelsAllowed BOOL, AllowRedirect BOOL, AllowedToRedirectAreBlacklist BOOL, AllowToRedirect TEXT, AllowFileTransfer BOOL, AllowFileTransferAreBlackList BOOL, AllowTransferTo TEXT)", params![]);

        match result {
            Ok(_) => {
                println!("Successfully initialize ClientCommandsToSend table!");
            },
            Err(e) => {
                eprintln!("An error occurred while scheduling the command in the ClientCommandsToSend table: {}", e);
            },
        };

        ((), conn)
    }).await;
}

#[derive(Debug, Clone)]
pub enum GroupError {
    GroupDoesNotExist(String),
    GroupAlreadyExist(String),
}

#[derive(Debug, Clone)]
pub struct PermissionGroup {
    group_id: Option<u32>,
    group_name: String,
    clients_allowed_to_use: Vec<String>,
    allowed_callbacks: Vec<String>,
    allow_create_new_clients: bool,
    allow_create_sub_channels: bool,

    max_sub_channels_allowed: bool,

    allow_redirect: bool,
    allowed_to_redirect_are_blacklist: bool,
    allow_redirect_to: Vec<String>,

    allow_file_transfer: bool,
    allow_transfer_to_are_blacklist: bool,
    allow_transfer_to: Vec<String>,
}

impl PermissionGroup {
    fn from(
        group_id: Option<u32>,
        group_name: String,
        clients_allowed_to_use: Vec<String>,
        allowed_callbacks: Vec<String>,
        allow_create_new_clients: bool,
        allow_create_sub_channels: bool,

        max_sub_channels_allowed: bool,

        allow_redirect: bool,
        allowed_to_redirect_are_blacklist: bool,
        allow_redirect_to: Vec<String>,

        allow_file_transfer: bool,
        allow_transfer_to_are_blacklist: bool,
        allow_transfer_to: Vec<String>,
    ) -> Self {
        let group = Self {
            group_id,
            group_name,
            clients_allowed_to_use,
            allowed_callbacks,
            allow_create_new_clients,
            allow_create_sub_channels,

            max_sub_channels_allowed,

            allow_redirect,
            allowed_to_redirect_are_blacklist,
            allow_redirect_to,

            allow_file_transfer,
            allow_transfer_to_are_blacklist,
            allow_transfer_to,
        };
        group
    }

    pub async fn create(
        group_name: String,
        clients_allowed_to_use: Vec<String>,
        allowed_callbacks: Vec<String>,
        allow_create_new_clients: bool,
        allow_create_sub_channels: bool,

        max_sub_channels_allowed: bool,

        allow_redirect: bool,
        allowed_to_redirect_are_blacklist: bool,
        allow_redirect_to: Vec<String>,

        allow_file_transfer: bool,
        allow_transfer_to_are_blacklist: bool,
        allow_transfer_to: Vec<String>,
    ) -> Result<PermissionGroup, GroupError> {
        if check_if_permission_group_name_exists(group_name.clone()).await {
            return Err(GroupError::GroupAlreadyExist(group_name));
        }

        let registered_ids = get_registred_ids();

        with_connection!(SQL_POOL, |conn: rusqlite::Connection| async {
            // let now = Utc::now();
            // let timestamp = now.timestamp() as f64 + (now.timestamp_subsec_millis() as f64 / 1000.0);

            let result = conn.execute(
                "INSERT INTO PermissionGroups (GroupName, 
                                               ClientAllowedToUse, 
                                               AllowedCallbacks, 
                                               AllowCreateNewClients, 
                                               AllowCreateSubChannels, 
                                               MaxSubChannelsAllowed, 
                                               AllowRedirect, 
                                               RedirectoToWhitelistIsBlacklist, 
                                               AllowRedirectTo, 
                                               AllowFileTransfer, 
                                               FileTransferWithelistAreBlackList, 
                                               AllowFileTransferTo) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
                params![
                    group_name,
                    serde_json::to_string(&clients_allowed_to_use).unwrap(),
                    serde_json::to_string(&allowed_callbacks).unwrap(),
                    allow_create_new_clients,
                    allow_create_sub_channels,
                    max_sub_channels_allowed,
                    allow_redirect,
                    allowed_to_redirect_are_blacklist,
                    serde_json::to_string(&allow_redirect_to).unwrap(),
                    allow_file_transfer,
                    allow_transfer_to_are_blacklist,
                    serde_json::to_string(&allow_transfer_to).unwrap(),
                ],
            );

            match result {
                Ok(rows) => {
                    if rows > 0 {
                        println!("Successfully inserted Log in the table PermissionGroups. {} row(s) were affected.", rows);
                    } else {
                        println!("No rows were affected.");
                    }
                },
                Err(e) => {
                    eprintln!("An error occurred while inserting the Log in the table PermissionGroups: {}", e);
                },
            };

            ((), conn)
        })
        .await;

        let group = Self {
            group_id: None,
            group_name,
            clients_allowed_to_use,
            allowed_callbacks,
            allow_create_new_clients,
            allow_create_sub_channels,

            max_sub_channels_allowed,

            allow_redirect,
            allowed_to_redirect_are_blacklist,
            allow_redirect_to,

            allow_file_transfer,
            allow_transfer_to_are_blacklist,
            allow_transfer_to,
        };
        Ok(group)
    }

    pub async fn from_name(group_name: String) -> Result<Self, GroupError> {
        with_connection!(SQL_POOL, |conn: rusqlite::Connection| async {
            let mut groups: Vec<PermissionGroup> = Vec::new();

            {
                let mut smtp = conn.prepare("SELECT * FROM PermissionGroups WHERE GroupName = ?").unwrap();

                let permission_groups_iter = smtp
                    .query_map(params![group_name], |row| {
                        Ok(PermissionGroup::from(
                            row.get(0).unwrap(),
                            row.get(1).unwrap(),
                            serde_json::from_str::<Vec<String>>(row.get::<_, String>(2)?.as_str()).unwrap(),
                            serde_json::from_str::<Vec<String>>(row.get::<_, String>(3)?.as_str()).unwrap(),
                            row.get(4).unwrap(),
                            row.get(5).unwrap(),
                            row.get(6).unwrap(),
                            row.get(7).unwrap(),
                            row.get(8).unwrap(),
                            serde_json::from_str::<Vec<String>>(row.get::<_, String>(9)?.as_str()).unwrap(),
                            row.get(10).unwrap(),
                            row.get(11).unwrap(),
                            serde_json::from_str::<Vec<String>>(row.get::<_, String>(12)?.as_str()).unwrap(),
                        ))
                    })
                    .unwrap();

                for permission_group in permission_groups_iter {
                    groups.push(permission_group.unwrap());
                }
            }

            let result = {
                if groups.len() == 0 {
                    Err(GroupError::GroupDoesNotExist(group_name))
                } else {
                    Ok(groups[0].clone())
                }
            };

            (result, conn)
        })
        .await
    }

    pub async fn edit(
        &self,
        group_name: String,
        clients_allowed_to_use: Vec<String>,
        allowed_callbacks: Vec<String>,
        allow_create_new_clients: bool,
        allow_create_sub_channels: bool,

        max_sub_channels_allowed: bool,

        allow_redirect: bool,
        allowed_to_redirect_are_blacklist: bool,
        allow_redirect_to: Vec<String>,

        allow_file_transfer: bool,
        allow_transfer_to_are_blacklist: bool,
        allow_transfer_to: Vec<String>,
    ) -> Result<PermissionGroup, GroupError> {
        let group_id = self.group_id;

        with_connection!(SQL_POOL, |conn: rusqlite::Connection| async {
            let result = conn.execute(
                "UPDATE PermissionGroups SET ID,
                GroupName = ?, 
                ClientAllowedToUse = ?, 
                AllowedCallbacks = ?, 
                AllowCreateNewClients = ?, 
                AllowCreateSubChannels = ?, 
                MaxSubChannelsAllowed = ?, 
                AllowRedirect = ?, 
                RedirectoToWhitelistIsBlacklist = ?, 
                AllowRedirectTo = ?, 
                AllowFileTransfer = ?, 
                FileTransferWithelistAreBlackList = ?, 
                AllowFileTransferTo = ? WHERE ID = ?;",
                params![
                    group_name,
                    serde_json::to_string(&clients_allowed_to_use).unwrap(),
                    serde_json::to_string(&allowed_callbacks).unwrap(),
                    allow_create_new_clients,
                    allow_create_sub_channels,
                    max_sub_channels_allowed,
                    allow_redirect,
                    allowed_to_redirect_are_blacklist,
                    serde_json::to_string(&allow_redirect_to).unwrap(),
                    allow_file_transfer,
                    allow_transfer_to_are_blacklist,
                    serde_json::to_string(&allow_transfer_to).unwrap(),
                    group_id.clone(),
                ],
            );

            match result {
                Ok(rows) => {
                    if rows > 0 {
                        println!("Successfully update PermissionGroups: {} in databse", group_name);
                    }
                },
                Err(e) => {
                    eprintln!("Error while update PermissionGroups: {} in the databse, the error is: {}", group_name, e);
                },
            }

            ((), conn)
        })
        .await;

        let new_group = Self {
            group_id,
            group_name,
            clients_allowed_to_use,
            allowed_callbacks,
            allow_create_new_clients,
            allow_create_sub_channels,

            max_sub_channels_allowed,

            allow_redirect,
            allowed_to_redirect_are_blacklist,
            allow_redirect_to,

            allow_file_transfer,
            allow_transfer_to_are_blacklist,
            allow_transfer_to,
        };

        Ok(new_group)
    }

    pub async fn delete(self) -> Result<(), GroupError> {
        with_connection!(SQL_POOL, |conn: rusqlite::Connection| async {
            let result = conn.execute("DELETE from PermissionGroups WHERE ID = ?", params![self.group_id]);

            match result {
                Ok(rows) => {
                    println!("Successfully deleted PermissionGroups: {} from groups! {} Rows were affected.", self.group_name, rows);
                },
                Err(e) => {
                    eprintln!("An error occurred while deleting PermissionGroups: {} from groups! And the error was: {}", self.group_name, e);
                },
            }

            ((), conn)
        })
        .await;
        Ok(())
    }
}

pub async fn check_if_permission_group_name_exists(group_name: String) -> bool {
    let group_names: Vec<String> = get_permission_group_names_registred().await;

    if group_names.contains(&group_name) {
        return true;
    } else {
        return false;
    }
}

async fn get_permission_group_names_registred() -> Vec<String> {
    with_connection!(SQL_POOL, |conn: rusqlite::Connection| async {
        let mut names: Vec<String> = Vec::new();
        {
            let mut smtp: Statement<'_> = conn.prepare("SELECT * FROM PermissionGroups").unwrap();
            let names_iter = smtp
                .query_map(params![], |row: &Row<'_>| {
                    let name: String = row.get(1)?;
                    Ok(name)
                })
                .unwrap();

            for name in names_iter {
                names.push(name.unwrap());
            }
        }

        (names, conn)
    })
    .await
}

async fn get_registred_ids() -> Vec<u32> {
    with_connection!(SQL_POOL, |conn: rusqlite::Connection| async {
        let mut ids: Vec<u32> = Vec::new();

        {
            let mut smtp = conn.prepare("SELECT * FROM PermissionGroups").unwrap();
            let commands_iter = smtp
                .query_map(params![], |row| {
                    let id: u32 = row.get(0)?;
                    Ok(id)
                })
                .unwrap();

            for command in commands_iter {
                ids.push(command.unwrap());
            }
        }

        (ids, conn)
    })
    .await
}

// pub fn registry_permission_group(
//     group_id: u32,
//     group_name: String,
//     clients_allowed_to_use: Vec<String>,
//     allowed_callbacks: Vec<String>,
//     allow_create_new_clients: bool,
//     allow_create_sub_channels: bool,

//     max_sub_channels_allowed: bool,

//     allow_redirect: bool,
//     allowed_to_redirect_are_blacklist: bool,
//     allow_redirect_to: Vec<String>,

//     allow_file_transfer: bool,
//     allow_transfer_to_are_blacklist: bool,
//     allow_transfer_to: Vec<String>,
// ) {
//     with_connection!(SQL_POOL, |conn: &rusqlite::Connection| {
//         // let now = Utc::now();
//         // let timestamp = now.timestamp() as f64 + (now.timestamp_subsec_millis() as f64 / 1000.0);

//         let registered_ids = get_registred_ids();

//         let mut id_generator = UniqueIdGenerator { registered_ids: registered_ids };

//         let result = conn.execute(
//             "INSERT INTO PermissionGroups (ID,
//                                            GroupName,
//                                            ClientAllowedToUse,
//                                            AllowedCallbacks,
//                                            AllowCreateNewClients,
//                                            AllowCreateSubChannels,
//                                            MaxSubChannelsAllowed,
//                                            AllowRedirect,
//                                            RedirectoToWhitelistIsBlacklist,
//                                            AllowRedirectTo,
//                                            AllowFileTransfer,
//                                            FileTransferWithelistAreBlackList,
//                                            AllowFileTransferTo) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
//             params![
//                 id_generator.gen(),
//                 group_name,
//                 serde_json::to_string(&clients_allowed_to_use).unwrap(),
//                 serde_json::to_string(&allowed_callbacks).unwrap(),
//                 allow_create_new_clients,
//                 allow_create_sub_channels,
//                 max_sub_channels_allowed,
//                 allow_redirect,
//                 allowed_to_redirect_are_blacklist,
//                 serde_json::to_string(&allow_redirect_to).unwrap(),
//                 allow_file_transfer,
//                 allow_transfer_to_are_blacklist,
//                 serde_json::to_string(&allow_transfer_to).unwrap(),
//             ],
//         );

//         match result {
//             Ok(rows) => {
//                 if rows > 0 {
//                     println!("Successfully inserted Log in the table PermissionGroups. {} row(s) were affected.", rows);
//                 } else {
//                     println!("No rows were affected.");
//                 }
//             },
//             Err(e) => {
//                 eprintln!("An error occurred while inserting the Log in the table PermissionGroups: {}", e);
//             },
//         };
//     })
// }

// fn get_permission_group_by_name(group_name: String) -> Result<Client, ClientError> {
//     with_connection!(SQL_POOL, |conn: &rusqlite::Connection| {
//         let mut groups: Vec<PermissionGroup> = Vec::new();

//         {
//             let mut smtp = conn.prepare("SELECT * FROM PermissionGroups WHERE GroupName = ?").unwrap();

//             let permission_groups_iter = smtp
//                 .query_map(params![group_name], |row| {
//                     Ok(PermissionGroup::from(
//                         row.get(0).unwrap(),
//                         row.get(1).unwrap(),
//                         serde_json::from_str::<Vec<String>>(row.get::<_, String>(2)?.as_str()).unwrap(),
//                         serde_json::from_str::<Vec<String>>(row.get::<_, String>(3)?.as_str()).unwrap(),
//                         row.get(4).unwrap(),
//                         row.get(5).unwrap(),
//                         row.get(6).unwrap(),
//                         row.get(7).unwrap(),
//                         row.get(8).unwrap(),
//                         serde_json::from_str::<Vec<String>>(row.get::<_, String>(9)?.as_str()).unwrap(),
//                         row.get(10).unwrap(),
//                         row.get(11).unwrap(),
//                         serde_json::from_str::<Vec<String>>(row.get::<_, String>(12)?.as_str()).unwrap(),
//                     ))
//                 })
//                 .unwrap();

//             for permission_group in permission_groups_iter {
//                 groups.push(permission_group.unwrap());
//             }
//         }

//         if groups.len() == 0 {
//             return Err(GroupError::GroupDoesNotExist(group_name));
//         } else {
//             return Ok(groups[0].clone());
//         }
//     })
// }

// pub fn edit_permission_group(group: PermissionGroup) {
//     with_connection!(SQL_POOL, |conn: &rusqlite::Connection| {
//         let result = conn.execute(
//             "UPDATE PermissionGroups SET ID,
//             GroupName = ?,
//             ClientAllowedToUse = ?,
//             AllowedCallbacks = ?,
//             AllowCreateNewClients = ?,
//             AllowCreateSubChannels = ?,
//             MaxSubChannelsAllowed = ?,
//             AllowRedirect = ?,
//             RedirectoToWhitelistIsBlacklist = ?,
//             AllowRedirectTo = ?,
//             AllowFileTransfer = ?,
//             FileTransferWithelistAreBlackList = ?,
//             AllowFileTransferTo = ? WHERE ID = ?;",
//             params![
//                 group.group_name,
//                 serde_json::to_string(&group.clients_allowed_to_use).unwrap(),
//                 serde_json::to_string(&group.allowed_callbacks).unwrap(),
//                 group.allow_create_new_clients,
//                 group.allow_create_sub_channels,
//                 group.max_sub_channels_allowed,
//                 group.allow_redirect,
//                 group.allowed_to_redirect_are_blacklist,
//                 serde_json::to_string(&group.allow_redirect_to).unwrap(),
//                 group.allow_file_transfer,
//                 group.allow_transfer_to_are_blacklist,
//                 serde_json::to_string(&group.allow_transfer_to).unwrap(),
//                 group.group_id,
//             ],
//         );

//         match result {
//             Ok(rows) => {
//                 if rows > 0 {
//                     println!("Successfully update PermissionGroups: {} in databse", group.group_name);
//                 }
//             },
//             Err(e) => {
//                 eprintln!("Error while update PermissionGroups: {} in the databse, the error is: {}", group.group_name, e);
//             },
//         }
//     });
// }

async fn remove_permission_group(group: PermissionGroup) {
    with_connection!(SQL_POOL, |conn: rusqlite::Connection| async {
        let result = conn.execute("DELETE from PermissionGroups WHERE ID = ?", params![group.group_id]);

        match result {
            Ok(rows) => {
                println!("Successfully deleted PermissionGroups: {} from groups! {} Rows were affected.", group.group_name, rows);
            },
            Err(e) => {
                eprintln!("An error occurred while deleting PermissionGroups: {} from groups! And the error was: {}", group.group_name, e);
            },
        }

        ((), conn)
    })
    .await;
}
