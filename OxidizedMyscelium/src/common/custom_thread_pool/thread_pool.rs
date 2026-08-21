// SPDX-License-Identifier: MPL-2.0
// Copyright © 2021-2026 Cristian Camargo Filho

use lazy_static::lazy_static;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Condvar, Mutex};
use std::thread;

lazy_static! {
    static ref DEBUG_MODE: bool = true; // Set this to true or false based on your needs
}

/// Represents a job that can be executed by the thread pool.
type Job = Option<Box<dyn FnOnce() + Send + 'static>>;

/// A unified thread pool for managing and executing tasks concurrently.
///
/// This thread pool provides mechanisms to:
/// - Execute tasks concurrently using worker threads.
/// - Wait for a free worker thread before executing a task.
/// - Gracefully shut down and stop the workers.
/// - Track the status of workers (busy or free).
pub struct UnifiedThreadPool {
    workers: Vec<Worker>,
    sender: mpsc::Sender<Job>,
    free_condvar: Arc<Condvar>,
    stopped: Arc<AtomicBool>,
    task_count: Arc<AtomicUsize>,
}

/// Represents an individual worker in the `UnifiedThreadPool`.
///
/// Each worker runs in its own thread and is responsible for executing jobs.
/// The worker maintains a status indicating whether it is busy or free.
struct Worker {
    id: usize,
    thread: Option<thread::JoinHandle<()>>,
    busy: Arc<AtomicBool>,
}

impl UnifiedThreadPool {
    /// Creates a new thread pool with the specified number of worker threads.
    ///
    /// # Arguments
    ///
    /// * `size` - The number of worker threads to create.
    ///
    /// # Returns
    ///
    /// * A new instance of `UnifiedThreadPool`.
    pub fn new(size: usize) -> UnifiedThreadPool {
        assert!(size > 0);

        let (sender, receiver) = mpsc::channel();
        let receiver = Arc::new(Mutex::new(receiver));
        let free_condvar = Arc::new(Condvar::new());
        let task_count = Arc::new(AtomicUsize::new(0)); // Initialize the task_count

        let mut workers = Vec::with_capacity(size);

        for id in 0..size {
            workers.push(Worker::new(
                id,
                Arc::clone(&receiver),
                Arc::clone(&free_condvar),
                Arc::clone(&task_count), // Pass the cloned task_count here
            ));
        }

        UnifiedThreadPool {
            workers,
            sender,
            free_condvar,
            stopped: Arc::new(AtomicBool::new(false)),
            task_count, // Add this line
        }
    }

    /// Executes a task in the thread pool.
    ///
    /// If the thread pool has been stopped, this method will not execute the task.
    ///
    /// # Arguments
    ///
    /// * `f` - The task to be executed.
    pub fn execute<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.task_count.fetch_add(1, Ordering::SeqCst);
        // Check if the pool has been stopped
        if self.stopped.load(Ordering::SeqCst) {
            if *DEBUG_MODE {
                println!("Pool has been stopped. Not sending job.");
            }
            return;
        }

        let job = Box::new(f);
        if let Err(err) = self.sender.send(Some(job)) {
            if *DEBUG_MODE {
                println!("Error sending job to worker: {:?}", err);
            }
        }
    }

    /// Waits for a worker thread to become free and then optionally executes a task.
    ///
    /// # Arguments
    ///
    /// * `f` - An optional task to be executed once a worker becomes free.
    pub fn wait_for_free_worker(&self, f: Job) {
        let lock = Mutex::new(());
        let mut guard = lock.lock().unwrap();
        while self.free_workers().is_empty() {
            guard = self
                .free_condvar
                .wait_timeout(guard, std::time::Duration::from_secs(1))
                .unwrap()
                .0;
        }
        if let Some(func) = f {
            self.execute(func);
        }
    }

    /// Returns a list of worker thread IDs that are currently free.
    ///
    /// # Returns
    ///
    /// * A `Vec<usize>` containing IDs of the free workers.
    pub fn free_workers(&self) -> Vec<usize> {
        self.workers
            .iter()
            .filter(|worker| !worker.busy.load(Ordering::SeqCst))
            .map(|worker| worker.id)
            .collect()
    }

    /// Helper method to check if all worker threads are free.
    ///
    /// # Returns
    ///
    /// * `true` if all workers are free, `false` otherwise.
    fn all_workers_free(&self) -> bool {
        self.workers
            .iter()
            .all(|worker| !worker.busy.load(Ordering::SeqCst))
    }

    /// Waits for all worker threads to become free.
    ///
    /// This is a blocking operation that waits until all workers have finished their tasks.
    pub fn join(&self) {
        let lock = Mutex::new(());
        let mut guard = lock.lock().unwrap();
        while !self.all_workers_free() {
            guard = self
                .free_condvar
                .wait_timeout(guard, std::time::Duration::from_millis(10))
                .unwrap()
                .0;
        }
    }

    /// Gracefully stops all worker threads.
    ///
    /// This method sends a termination message to all workers and waits for them to finish their current tasks.
    pub fn stop(&mut self) {
        if !self.stopped.load(Ordering::SeqCst) {
            if *DEBUG_MODE {
                println!("Sending terminate message to all workers.");
            }

            for _ in &self.workers {
                if let Err(err) = self.sender.send(None) {
                    if *DEBUG_MODE {
                        println!("Error sending terminate message to worker: {:?}", err);
                    }
                }
            }

            if *DEBUG_MODE {
                println!("Shutting down all workers.");
            }

            for worker in &mut self.workers {
                if *DEBUG_MODE {
                    println!("Shutting down worker {}", worker.id);
                }

                if let Some(thread) = worker.thread.take() {
                    thread.join().unwrap();
                }
            }

            self.stopped.store(true, Ordering::SeqCst);
        }
    }
}

impl Worker {
    /// Creates a new worker with a unique ID.
    ///
    /// The worker will listen for tasks from the shared job receiver and execute them.
    ///
    /// # Arguments
    ///
    /// * `id` - The unique ID for the worker.
    /// * `receiver` - The shared job receiver from which the worker fetches tasks.
    /// * `free_condvar` - A condition variable used to notify when the worker becomes free.
    /// * `task_count` - An atomic counter tracking the number of pending tasks.
    ///
    /// # Returns
    ///
    /// * A new instance of `Worker`.
    fn new(
        id: usize,
        receiver: Arc<Mutex<mpsc::Receiver<Job>>>,
        free_condvar: Arc<Condvar>,
        task_count: Arc<AtomicUsize>,
    ) -> Worker {
        let busy = Arc::new(AtomicBool::new(false));
        let busy_clone = Arc::clone(&busy);
        let free_condvar_clone = Arc::clone(&free_condvar);
        let task_count_clone = Arc::clone(&task_count);

        let thread = thread::spawn(move || loop {
            let job = match receiver.lock().unwrap().recv() {
                Ok(Some(job)) => {
                    task_count_clone.fetch_sub(1, Ordering::SeqCst);
                    job
                }
                Ok(None) => return,
                Err(_) => return,
            };

            if *DEBUG_MODE {
                println!("Unified Worker {} got a job; executing.", id);
            }
            busy_clone.store(true, Ordering::SeqCst);
            job();
            busy_clone.store(false, Ordering::SeqCst);
            free_condvar_clone.notify_one();
        });

        Worker {
            id,
            thread: Some(thread),
            busy,
        }
    }
}

impl Drop for UnifiedThreadPool {
    /// Ensures that all worker threads are stopped when the thread pool is dropped.
    fn drop(&mut self) {
        self.stop();
    }
}
