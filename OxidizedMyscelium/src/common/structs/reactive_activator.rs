use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::task::{spawn_blocking, yield_now};
use tokio::time::{sleep, Duration};
use tokio::{
    sync::{Mutex, Semaphore},
    task::JoinHandle,
};

use syn::token::Mut;

pub struct ReactiveActivator {
    shutdown: Arc<std::sync::atomic::AtomicBool>,
    thread_handle: Arc<Mutex<Option<JoinHandle<()>>>>, // ← Tokio JoinHandle
    action: Arc<dyn Fn() + Send + Sync>,
    condition: Arc<dyn Fn() -> Pin<Box<dyn Future<Output = bool> + Send>> + Send + Sync>,
    sem: Arc<Semaphore>, // pass in the semaphore you want to use
}

impl fmt::Debug for ReactiveActivator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReactiveActivator")
            .field("thread_handle", &self.thread_handle) // this *is* Debug
            .field("action", &"<dyn Fn>") // placeholder
            .field("condition", &"<dyn Fn -> bool>") // placeholder
            .finish()
    }
}

impl ReactiveActivator {
    pub fn new(
        action: Arc<dyn Fn() + Send + Sync>,
        condition: Arc<dyn Fn() -> Pin<Box<dyn Future<Output = bool> + Send>> + Send + Sync + 'static>,
        sem: Arc<Semaphore>, // pass in the semaphore you want to use
    ) -> Arc<Self> {
        Arc::new(Self {
            thread_handle: Arc::new(Mutex::new(None)),
            action,
            condition,
            sem,
            shutdown: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        })
    }

    /// Starts the background task if not already running.
    pub async fn start(&self) {
        let mut guard = self.thread_handle.lock().await;
        if guard.is_some() {
            return;
        }

        let action = Arc::clone(&self.action);
        let condition = Arc::clone(&self.condition);
        let sem = Arc::clone(&self.sem);
        let shutdown_flag = Arc::clone(&self.shutdown);
        let handle_ref = Arc::clone(&self.thread_handle);

        let handle: JoinHandle<()> = tokio::spawn(async move {
            loop {
                // Acquire permit per iteration
                let permit = sem.acquire().await.expect("semaphore closed");

                // Run action in blocking pool
                let action_clone = Arc::clone(&action);
                let _ = spawn_blocking(move || {
                    (action_clone)();
                })
                .await;

                // Release permit
                drop(permit);

                // Check stop condition
                if shutdown_flag.load(std::sync::atomic::Ordering::SeqCst) || (condition)().await {
                    break;
                }

                // Back-off to allow other tasks/time
                sleep(Duration::from_millis(500)).await;
            }

            // On natural exit, clear the handle
            let mut guard = handle_ref.lock().await;
            *guard = None;
        });

        *guard = Some(handle);
    }

    /// Stops the task if it’s running, waiting for it to finish.
    pub async fn stop(&self) {
        // 1. Signal the loop exit:
        self.shutdown.store(true, Ordering::SeqCst);
        self.sem.add_permits(1);

        // 2. extract the JoinHandle, then **drop the mutex guard**
        let join_handle_opt = {
            let mut guard = self.thread_handle.lock().await;
            guard.take() // move it out
        }; // guard dropped here

        // 3. now we can safely await the task
        if let Some(handle) = join_handle_opt {
            let _ = handle.await;
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

// ─── The module under test: ReactiveActivator & CloneableBox ───
// (Insert the code you already have here, e.g. `pub struct ReactiveActivator { … }`
//  and its impls, plus `pub struct CloneableBox<F>` etc.; we assume that’s already written.)

// ─── Tests ─────────────────────────────────────────────────────────────
#[cfg(test)]
mod tests {
    // bring everything from the outer module into scope:
    use super::*;

    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::sync::Barrier;
    use tokio::task::block_in_place;
    use tokio::time::{sleep, Duration};

    // For clarity, alias the macro:
    use tokio::test as tokio_test;

    // ─── Test 1: Ensure the ReactiveActivator loop calls action exactly N times and then stops ───
    #[tokio_test]
    async fn reactive_activator_runs_exact_number_of_times_then_stops() {
        use std::pin::Pin;
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        };
        use tokio::time::{sleep, Duration};

        // ───────────────────── ACTION (unchanged) ────────────────────────────────
        let action_count = Arc::new(AtomicUsize::new(0));
        let action_closure: Arc<dyn Fn() + Send + Sync + 'static> = {
            let action_count_clone = Arc::clone(&action_count);
            Arc::new(move || {
                action_count_clone.fetch_add(1, Ordering::SeqCst);
            })
        };

        // ───────────────────── CONDITION (async) ─────────────────────────────────
        //
        // Old signature:     Arc<dyn Fn() -> bool + Send + Sync>
        // New signature:     Arc<dyn Fn() -> Pin<Box<dyn Future<Output = bool> + Send>> + Send + Sync>
        //
        // We still want: “return `false` the first two times, then `true` the third.”
        let condition_count = Arc::new(AtomicUsize::new(0));
        let condition_closure: Arc<dyn Fn() -> Pin<Box<dyn std::future::Future<Output = bool> + Send>> + Send + Sync + 'static> = {
            let condition_count_clone = Arc::clone(&condition_count);
            Arc::new(move || {
                // We capture `prev` synchronously, then wrap it into a ready future.
                let prev = condition_count_clone.fetch_add(1, Ordering::SeqCst);
                Box::pin(async move {
                    // Returning `true` only once `prev >= 2` (i.e. on the 3rd call)
                    prev >= 2
                })
            })
        };

        // Initialize a semaphore to control the transpositon execution flow
        let transposer_sem = Arc::new(Semaphore::new(1));

        // ───────────────────── BUILD + START THE ACTIVATOR ──────────────────────
        let activator = ReactiveActivator::new(action_closure, condition_closure, transposer_sem);

        // Start the background loop:
        activator.start().await;

        // Wait 2 seconds (each loop iteration sleeps 500ms before re‐checking).
        // In roughly 1.0 second it would do 2 loops; by 1.5‐2.0 seconds it should do 3 loops and then break.
        sleep(Duration::from_millis(2000)).await;

        // By now, the loop should have run 3 times (and then stopped).
        let actions = action_count.load(Ordering::SeqCst);
        let cond_checks = condition_count.load(Ordering::SeqCst);

        // We expect exactly 3 calls to action(); on the third iteration, condition returned true.
        // Depending on timing, it's possible the loop does exactly 3, or maybe 4 if scheduling jitter occurs
        // (e.g. if condition() checks twice quickly). So we assert “>= 3 but not too many.”
        assert!(actions >= 3 && actions <= 4, "Expected action to be called ~3 times; saw {} times", actions);

        // The condition closure must have been called exactly the same number of times as action,
        // because each loop iteration does “action(); let cond = condition(); if cond { break; }”.
        assert!(cond_checks >= 3 && cond_checks <= 4, "Expected condition to be checked ~3 times; saw {} times", cond_checks);

        // Now that the loop has broken out, it should have cleared its internal JoinHandle.
        // We can check that `activator.thread_handle` is now None.
        {
            let guard = activator.thread_handle.lock().await;
            assert!(guard.is_none(), "Expected thread_handle to be None after loop exit");
        }

        // Finally, calling `.stop()` should do nothing harmful (since the handle is already None).
        activator.stop().await;
        // (No panic is considered success.)
    }

    // ─── Test 2: Ensure multiple calls to start() don't spawn duplicate tasks ───

    // Helper to create a condition that always returns false (for continuous looping)
    fn always_false_condition() -> Arc<dyn Fn() -> Pin<Box<dyn Future<Output = bool> + Send>> + Send + Sync + 'static> {
        Arc::new(|| {
            Box::pin(async {
                sleep(Duration::from_millis(10)).await; // Small delay to allow scheduler to switch
                false
            })
        })
    }

    // Helper to create a condition that returns true immediately (for quick stopping)
    fn always_true_condition() -> Arc<dyn Fn() -> Pin<Box<dyn Future<Output = bool> + Send>> + Send + Sync + 'static> {
        Arc::new(|| Box::pin(async { true }))
    }

    // 2: “Semaphore actually limits concurrent action() calls”
    #[tokio::test]
    async fn test_semaphore_concurrency_limit() {
        println!("--- test_semaphore_concurrency_limit ---");

        // Shared counters to measure “running”, “max concurrent”, and “finished”:
        let running_count = Arc::new(AtomicUsize::new(0));
        let max_concurrent = Arc::new(AtomicUsize::new(0));
        let finished_count = Arc::new(AtomicUsize::new(0));

        // Limit to 2 simultaneous permits:
        let num_permits = 2;
        let shared_sem = Arc::new(Semaphore::new(num_permits));
        println!("  Semaphore initialized with {} permits.", num_permits);

        // Define a blocking‐work action. Wrap the sleep in block_in_place()
        // so we do NOT stall Tokio’s core thread.
        let action: Arc<dyn Fn() + Send + Sync + 'static> = {
            // Explicit type annotation is good practice
            // Use shorter, more descriptive names for the cloned Arcs if they're only used here
            let running_actions = Arc::clone(&running_count);
            let max_active_actions = Arc::clone(&max_concurrent);
            let completed_actions = Arc::clone(&finished_count);

            Arc::new(move || {
                // Increment count *before* potentially entering the critical section or work
                let current_active = running_actions.fetch_add(1, Ordering::SeqCst);
                // Update max observed concurrently
                max_active_actions.fetch_max(current_active + 1, Ordering::SeqCst);

                println!("    [Action] started; active = {}", current_active + 1);

                // Use block_in_place for synchronous, CPU-bound or blocking I/O work.
                // It offloads the work to a dedicated blocking thread pool, preventing
                // the Tokio reactor from being stalled.
                block_in_place(|| {
                    // Simulate work that takes time (e.g., CPU computation, synchronous I/O)
                    std::thread::sleep(Duration::from_millis(50));
                });

                // Decrement count *after* work is conceptually finished
                running_actions.fetch_sub(1, Ordering::SeqCst);
                completed_actions.fetch_add(1, Ordering::SeqCst);

                let remaining_active = running_actions.load(Ordering::SeqCst);
                println!("    [Action] finished; remaining active = {}", remaining_active);
            })
        };

        // Spawn 5 activators, all sharing the same 2‐permit semaphore:
        let mut activators = Vec::new();
        let total_instances = 5;
        println!("  Creating {} ReactiveActivator instances.", total_instances);
        for _ in 0..total_instances {
            let act = ReactiveActivator::new(Arc::clone(&action), always_false_condition(), Arc::clone(&shared_sem));
            activators.push(act);
        }

        // Start all activators in parallel (each in its own Tokio task):
        println!("  Starting all {} activators concurrently.", total_instances);
        let mut join_handles = Vec::new();
        for a in &activators {
            let a_clone = Arc::clone(a);
            join_handles.push(tokio::spawn(async move {
                a_clone.start().await;
            }));
        }

        // Wait 500 ms so each activator can cycle through acquire→action()→release:
        sleep(Duration::from_millis(500)).await;

        // Check that we never saw more than 2 simultaneous actions:
        let observed_max = max_concurrent.load(Ordering::SeqCst);
        println!("  Observed max concurrent = {}", observed_max);
        assert!(observed_max <= num_permits, "Expected ≤ {} concurrent actions, but saw {}", num_permits, observed_max);

        // Check that at least one action ran (i.e. finished_count > 0)
        let total_finished = finished_count.load(Ordering::SeqCst);
        println!("  Total finished actions = {}", total_finished);
        assert!(total_finished > 0, "Expected at least one action to finish, but none did.");

        // Now stop all activators:
        println!("  Stopping all activators...");
        for a in activators {
            a.stop().await;
        }

        // Ensure each spawned “start” task has returned:
        for handle in join_handles {
            handle.await.expect("join error");
        }
        println!("  All activators stopped.");

        // After stopping, there should be no “running” actions left:
        let running_now = running_count.load(Ordering::SeqCst);
        assert_eq!(running_now, 0, "Expected 0 running actions after stop(), but found {}", running_now);

        // Finally, verify the semaphore still works by acquiring + releasing one permit:
        println!("  Verifying semaphore is still usable post‐test...");
        let permit = shared_sem.acquire().await.expect("couldn't acquire permit");
        drop(permit);
        println!("  Semaphore permit acquired & released. ✅");

        println!("--- test_semaphore_concurrency_limit complete ---");
    }

    // ─── Test 3: Ensure stop() actually awaits the background loop if it hasn’t finished yet ───
    #[tokio::test]
    async fn reactive_activator_stop_waits_for_loop_to_finish() {
        // Let’s create a condition that only becomes true after a small delay.
        // Meanwhile, we want to call stop() *before* the loop naturally clears itself,
        // and check that stop() awaits the loop’s end.

        use std::pin::Pin;
        use std::sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering},
            Arc,
        };
        use tokio::time::{sleep, Duration};

        // ─────────────────── ACTION COUNTER ────────────────────────────────
        let action_counter = Arc::new(AtomicUsize::new(0));
        let action_closure: Arc<dyn Fn() + Send + Sync + 'static> = {
            let counter_clone = Arc::clone(&action_counter);
            Arc::new(move || {
                counter_clone.fetch_add(1, Ordering::SeqCst);
            })
        };

        // ─────────────────── CONDITION (FLAG-BASED) ────────────────────────
        //
        // • cond_flag starts false.
        // • We flip it to true after 300 ms in a spawned task.
        // • The condition closure returns an *async ready* future with that flag.
        let cond_flag = Arc::new(AtomicBool::new(false));

        // spawn a task that sets the flag after 300 ms
        {
            let flag = Arc::clone(&cond_flag);
            tokio::spawn(async move {
                sleep(Duration::from_millis(300)).await;
                flag.store(true, Ordering::SeqCst);
            });
        }

        // async predicate: each call just reads the flag and returns a ready future
        let condition_closure: Arc<dyn Fn() -> Pin<Box<dyn std::future::Future<Output = bool> + Send>> + Send + Sync + 'static> = {
            let flag_clone = Arc::clone(&cond_flag);
            Arc::new(move || {
                let current = flag_clone.load(Ordering::SeqCst);
                Box::pin(async move { current }) // ready future
            })
        };

        // Initialize a semaphore to control the transpositon execution flow
        let transposer_sem = Arc::new(Semaphore::new(1));

        // ─────────────────── BUILD & START ACTIVATOR ───────────────────────
        let activator = ReactiveActivator::new(action_closure, condition_closure, transposer_sem);

        // Start the background loop:
        activator.start().await;

        // Wait 300ms, then flip the flag so the background loop can exit:
        sleep(Duration::from_millis(300)).await;
        cond_flag.store(true, Ordering::SeqCst);

        // Immediately call stop()—this should await the spawned task finishing.
        let stop_start = tokio::time::Instant::now();
        activator.stop().await;
        let stop_duration = stop_start.elapsed();

        // Because the loop checks condition once every 500ms, and we set the flag after 300ms,
        // the loop will do at most 1 more iteration (sleep 500ms) before noticing. So stop() must
        // have waited roughly ~200ms or so (i.e. until the next cond-check). We just assert it took
        // at least 100ms (prove that we did actually wait), and less than, say, 1 second.

        assert!(stop_duration.as_millis() >= 100, "Expected stop() to wait at least 100ms, but it returned in {}ms", stop_duration.as_millis());
        assert!(stop_duration.as_millis() < 1000, "Expected stop() to finish within 1s, but it took {}ms", stop_duration.as_millis());
    }

    // ─── Test 4: Simple test for CloneableBox ───
    #[tokio_test]
    async fn cloneable_box_indeed_shares_underlying_closure() {
        // 1) A shared counter
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();

        // 2) Build a `dyn Fn() + Send + Sync + 'static` that increments it
        let underlying: Arc<dyn Fn() + Send + Sync + 'static> = Arc::new(move || {
            counter_clone.fetch_add(1, Ordering::SeqCst);
        });

        // 3) Wrap it in a CloneableBox<dyn Fn() + Send + Sync>
        let box1: CloneableBox<dyn Fn() + Send + Sync> = CloneableBox::new(underlying.clone());
        let box2 = box1.clone();

        // 4) Invoke both closures via deref:
        (*box1)();
        (*box2)();

        // 5) Assert the counter was bumped twice
        assert_eq!(counter.load(Ordering::SeqCst), 2, "Expected the shared closure to be called twice");
    }

    #[tokio::test]
    async fn test_semaphore_limits_to_one_concurrent_execution() {
        let counter = Arc::new(AtomicUsize::new(0));
        let execution_log = Arc::new(Mutex::new(Vec::new()));
        let barrier = Arc::new(Barrier::new(2)); // for synchronization

        // Shared semaphore with only 1 permit
        let semaphore = Arc::new(Semaphore::new(1));

        let make_action = |id: usize| {
            let counter = Arc::clone(&counter);
            let log = Arc::clone(&execution_log);
            let barrier = Arc::clone(&barrier);

            Arc::new(move || {
                let count = counter.fetch_add(1, Ordering::SeqCst);
                println!("Action {} executed, count = {}", id, count);

                let mut log = log.blocking_lock();
                log.push(format!("Action {} ran at {}", id, count));

                // Wait until both actions are started
                let _ = barrier.wait();
            }) as Arc<dyn Fn() + Send + Sync>
        };

        fn make_condition() -> Arc<dyn Fn() -> Pin<Box<dyn Future<Output = bool> + Send>> + Send + Sync> {
            Arc::new(|| {
                Box::pin(async move {
                    sleep(Duration::from_millis(100)).await;
                    true
                })
            })
        }

        let condition = make_condition();

        let activator1 = ReactiveActivator::new(make_action(1), Arc::clone(&condition), Arc::clone(&semaphore));
        let activator2 = ReactiveActivator::new(make_action(2), Arc::clone(&condition), Arc::clone(&semaphore));

        // Start both at (nearly) the same time
        let t1 = tokio::spawn(async move { activator1.start().await });
        let t2 = tokio::spawn(async move { activator2.start().await });

        t1.await.unwrap();
        t2.await.unwrap();

        // Let them finish
        sleep(Duration::from_millis(150)).await;

        let log = execution_log.lock().await;

        println!("Execution log: {:?}", *log);

        // One should be blocked until the other ends
        assert_eq!(log.len(), 2); // both got executed eventually
        assert_ne!(log[0], log[1], "Both actions ran concurrently!");

        // Optional: enforce that they didn’t run *at the same time*
        let a1 = log.iter().any(|s| s.contains("Action 1"));
        let a2 = log.iter().any(|s| s.contains("Action 2"));
        assert!(a1 && a2, "Both actions should have been invoked sequentially");
    }
}
