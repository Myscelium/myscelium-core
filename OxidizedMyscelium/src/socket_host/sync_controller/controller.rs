use std::collections::HashMap;

use chrono::DateTime;
use lazy_static::lazy_static;

extern crate chrono;
use chrono::prelude::Utc;

use crate::chrono::TimeZone;
use std::sync::Arc;
use std::sync::Mutex;

use chrono::Duration;

use std::thread;
use std::time;

#[derive(Clone, Debug)]
pub struct Client {
    max_sync_attempts: u32,
    sync_status: bool,
    sync_attempts: u32,
    last_sync_request: i64,
    key: String,
}

#[derive(Clone, Debug)]
pub enum ClientStatusPoolError {
    ClientAlreadyExist(String),
    ClientDoesNotExist(String),
    MaxSyncAttemptsReached(String),
    ClientAlreadySync(String),
}

impl Client {
    pub fn new(client_key: String, max_sync_attempts: u32) -> Self {
        Self {
            max_sync_attempts,
            sync_status: false,
            sync_attempts: 0,
            last_sync_request: -1, // To mark as never sync
            key: client_key,
        }
    }

    pub fn update_client(new_client: Client) -> Self {
        Self {
            max_sync_attempts: new_client.max_sync_attempts,
            sync_status: new_client.sync_status,
            sync_attempts: new_client.sync_attempts,
            last_sync_request: new_client.last_sync_request,
            key: new_client.key,
        }
    }

    pub fn update_sync_attempt(&mut self) -> Result<(), ClientStatusPoolError> {
        let now = Utc::now();
        self.last_sync_request = now.timestamp_millis();

        if self.sync_attempts + 1 > self.max_sync_attempts {
            return Err(ClientStatusPoolError::MaxSyncAttemptsReached(
                self.get_key(),
            ));
        } else {
            self.sync_attempts += 1;
        }

        return Ok(());
    }

    pub fn reset_sync(&mut self) {
        self.sync_status = false;
        self.last_sync_request = -1;
        self.sync_attempts = 0;
    }

    pub fn get_last_sync_attempt(&mut self) -> i64 {
        self.last_sync_request
    }

    pub fn get_max_sync_attempts(&self) -> u32 {
        self.max_sync_attempts
    }

    pub fn get_sync_attempts(&mut self) -> u32 {
        self.sync_attempts
    }

    pub fn update_sync_status(&mut self, new_status: bool) {
        self.sync_status = new_status;
    }

    pub fn get_sync_status(&mut self) -> bool {
        self.sync_status
    }

    fn get_key(&self) -> String {
        self.key.clone()
    }
}

#[derive(Clone, Debug)]
pub struct Clients {
    clients: Vec<Client>,
}

impl Clients {
    pub fn new() -> Self {
        let clients: Vec<Client> = Vec::new();
        Self { clients }
    }

    pub fn get_remaining_sync_attempts(
        &mut self,
        client_key: &String,
    ) -> Result<u32, ClientStatusPoolError> {
        let client = self.get_client(client_key)?;
        return Ok(client.get_max_sync_attempts() - client.get_sync_attempts());
    }

    pub fn add_new_client(
        &mut self,
        client_key: String,
        max_sync_attempts: u32,
    ) -> Result<(), ClientStatusPoolError> {
        {
            for client in &self.clients {
                if client.get_key() == client_key {
                    return Err(ClientStatusPoolError::ClientAlreadyExist(client_key));
                }
            }
        }

        let client: Client = Client::new(client_key, max_sync_attempts);
        self.clients.push(client);
        Ok(())
    }
    pub fn get_client(
        &mut self,
        client_key: &String,
    ) -> Result<&mut Client, ClientStatusPoolError> {
        for client in &mut self.clients {
            if &client.get_key() == client_key {
                return Ok(client);
            }
        }
        return Err(ClientStatusPoolError::ClientDoesNotExist(
            client_key.clone(),
        ));
    }
    pub fn update_client_sync_attempt(
        &mut self,
        client_key: &String,
    ) -> Result<(), ClientStatusPoolError> {
        let client = self.get_client(client_key)?;

        if !client.get_sync_status() {
            let _ = client.update_sync_attempt()?;
            return Ok(());
        }

        return Err(ClientStatusPoolError::ClientAlreadySync(client_key.clone()));
    }
    pub fn update_client_sync_status(
        &mut self,
        client_key: &String,
        new_status: bool,
    ) -> Result<(), ClientStatusPoolError> {
        let mut client = self.get_client(client_key)?;
        client.update_sync_status(new_status);
        return Ok(());
    }

    pub fn update_sync_status_for_clients(
        &mut self,
        client_keys: Vec<String>,
        new_status: bool,
    ) -> Result<(), ClientStatusPoolError> {
        for client_key in client_keys {
            self.update_client_sync_status(&client_key, new_status)?;
        }
        Ok(())
    }

    pub fn reset_sync_for_client(
        &mut self,
        client_key: &String,
    ) -> Result<(), ClientStatusPoolError> {
        let client: &mut Client = self.get_client(client_key)?;
        client.reset_sync();
        Ok(())
    }

    pub fn get_sync_status(&mut self, client_key: &String) -> Result<bool, ClientStatusPoolError> {
        let client: &mut Client = self.get_client(client_key)?;
        Ok(client.get_sync_status())
    }

    pub fn get_last_sync(
        &mut self,
        client_key: &String,
    ) -> Result<DateTime<Utc>, ClientStatusPoolError> {
        let client: &mut Client = self.get_client(client_key)?;
        Ok(Utc
            .timestamp_millis_opt(client.get_last_sync_attempt())
            .unwrap())
    }
}
