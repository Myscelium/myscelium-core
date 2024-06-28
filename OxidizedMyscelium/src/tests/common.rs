// tests/common.rs

use ctor::ctor;
use std::sync::Once;

static INIT: Once = Once::new();

#[ctor]
fn setup() {
    println!("Setup before tests");
    // perform setup actions, e.g., initializing logging, creating test data, etc.
}

pub struct Teardown;

impl Drop for Teardown {
    fn drop(&mut self) {
        println!("Teardown after tests");
        // perform cleanup actions, e.g., deleting test data, closing connections, etc.
    }
}

pub fn run_once() {
    INIT.call_once(|| {
        // Ensure that setup only runs once
        let _teardown = Teardown;
    });
}
