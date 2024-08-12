use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

use syn::token::Mut;

pub struct ReactiveActivator {
    thread_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    action: Arc<dyn Fn() + Send + Sync>,
    condition: Arc<dyn Fn() -> bool + Send + Sync>,
}

impl ReactiveActivator {
    pub fn new(action: Arc<dyn Fn() + Send + Sync>, condition: Arc<dyn Fn() -> bool + Send + Sync>) -> Arc<Self> {
        Arc::new(Self {
            thread_handle: Arc::new(Mutex::new(None)),
            action,
            condition,
        })
    }

    // Change here: `&self` instead of `self`
    pub fn start(&self) {
        let mut handle = self.thread_handle.lock().unwrap();
        if handle.is_none() {
            let action = Arc::clone(&self.action);
            let condition = Arc::clone(&self.condition);
            let thread_handle = Arc::clone(&self.thread_handle); // Arc for thread handle

            println!("Starting the ReactiveActivator thread...");

            *handle = Some(thread::spawn(move || {
                loop {
                    println!("Executing action...");
                    action();
                    println!("Condition arrived in reactive activator: {:?}", condition());
                    if condition() {
                        println!("Condition failed, stopping loop.");
                        break;
                    }
                }
                // Clear the thread handle when the thread exits
                let mut handle = thread_handle.lock().unwrap();
                *handle = None;
            }));
        } else {
            println!("Thread already started.");
        }
    }

    pub fn stop(&self) {
        let mut handle = self.thread_handle.lock().unwrap();
        if let Some(handle) = handle.take() {
            handle.join().unwrap(); // Wait for the thread to finish
        }
    }
}

pub struct CloneableBox<F: ?Sized>(Arc<F>);

impl<F: ?Sized> Clone for CloneableBox<F>
where
    F: Fn() + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        CloneableBox(Arc::clone(&self.0))
    }
}

impl<F: ?Sized> CloneableBox<F>
where
    F: Fn() + Send + Sync + 'static,
{
    fn new(f: Arc<F>) -> Self {
        CloneableBox(f)
    }
}

impl<F: ?Sized> std::ops::Deref for CloneableBox<F>
where
    F: Fn() + Send + Sync + 'static,
{
    type Target = F;

    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}
