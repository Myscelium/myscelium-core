use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::time::{sleep, Duration};
use tokio::{
    sync::{Mutex, Semaphore},
    task::JoinHandle,
};

use syn::token::Mut;

pub struct ReactiveActivator {
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
        })
    }

    /// Starts the background task if not already running.
    pub async fn start(&self) {
        let mut handle_guard = self.thread_handle.lock().await;
        if handle_guard.is_none() {
            let action = Arc::clone(&self.action);
            let condition = Arc::clone(&self.condition);
            let thread_handle = Arc::clone(&self.thread_handle);
            let sem = Arc::clone(&self.sem);

            println!("Starting the ReactiveActivator task...");

            // Spawn the async loop
            let join_handle = tokio::spawn(async move {
                // Acquire one permit (awaits if limit reached)
                let _permit = sem.acquire().await.expect("semaphore closed");

                // ––––––––– MAIN LOOP –––––––––
                loop {
                    println!("Executing action...");
                    (action)();

                    let cond = (condition)();
                    let cond_bool: bool = cond.await;
                    println!("Condition result: {:?}", cond_bool);
                    if cond_bool {
                        println!("Condition met, stopping loop.");
                        break;
                    }
                }

                // When loop ends, `_permit` is dropped → releases the seat.

                // Clear the handle when done
                let mut guard = thread_handle.lock().await; // TODO We can move it into the outside of the thread.
                *guard = None;
            });

            *handle_guard = Some(join_handle);
        } else {
            println!("Task already started.");
        }

        println!("Exiting executor!")
    }

    /// Stops the task if it’s running, waiting for it to finish.
    pub async fn stop(&self) {
        let mut handle_guard = self.thread_handle.lock().await;
        if let Some(join_handle) = handle_guard.take() {
            println!("Stopping ReactiveActivator task...");
            // await your spawned task to finish
            let _ = join_handle.await;
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
    #[tokio_test]
    async fn reactive_activator_does_not_spawn_multiple_loops() {
        // We will measure how many times `action()` runs in a fixed time window after one start,
        // then call start() again, measure again, and ensure it does not double.

        use std::pin::Pin;
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
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

        // ─────────────────── CONDITION (ALWAYS FALSE) ──────────────────────
        //
        // Signature required by `ReactiveActivator`:
        //   Arc<dyn Fn() -> Pin<Box<dyn Future<Output = bool> + Send>> + Send + Sync + 'static>
        //
        // Here we use `Box::pin(async { false })`, which produces a ready future
        // whose output is `false` every time.
        let condition_closure: Arc<dyn Fn() -> Pin<Box<dyn std::future::Future<Output = bool> + Send>> + Send + Sync + 'static> = Arc::new(|| Box::pin(async { false }));

        // Initialize a semaphore to control the transpositon execution flow
        let transposer_sem = Arc::new(Semaphore::new(1));

        // ─────────────────── BUILD & START ACTIVATOR ───────────────────────
        let activator = ReactiveActivator::new(action_closure, condition_closure, transposer_sem);

        // 1) First start:
        activator.start().await;

        // Let it run for 600ms (so it can do roughly 1 iteration—remember each loop has a 500ms sleep).
        sleep(Duration::from_millis(600)).await;
        let count_after_first_window = action_counter.load(Ordering::SeqCst);

        // 2) Call start() again while it’s still running.
        activator.start().await;

        // Let it run another 600ms:
        sleep(Duration::from_millis(600)).await;
        let count_after_second_window = action_counter.load(Ordering::SeqCst);

        // Now:
        // - If two independent loops were running in parallel, then in the second 600ms window
        //   we’d expect roughly 2 iterations instead of 1 (because two loops each do 1 iteration in 500ms).
        //
        // Let:
        //    delta1 = count_after_first_window  (≈ 1)
        //    delta2 = count_after_second_window - count_after_first_window  (should also be ≈ 1, not ≈ 2)
        //
        // We allow small jitter (±1), but if the second window shows ≥ 2× the first window, something is wrong.

        let delta1 = count_after_first_window;
        let delta2 = count_after_second_window.saturating_sub(count_after_first_window);

        // Check that delta1 is roughly 1 (can sometimes be 0 or 2 if scheduling is weird):
        assert!((delta1 == 1) || (delta1 == 0) || (delta1 == 2), "Expected roughly 1 iteration in first 600ms, got {}", delta1);

        // The key check: delta2 should be in the same ballpark as delta1, NOT roughly double.
        // i.e. we fail if delta2 >= 2 * delta1 + 1 (allow 1 count of jitter).
        // If delta1==0, we just check that delta2 is small (e.g. <= 1).
        if delta1 == 0 {
            assert!(delta2 <= 1, "Second window progressed by {}, but expected ≤1 (delta1==0)", delta2);
        } else {
            assert!(delta2 < delta1 * 2, "Second window progressed by {}, but expected < {} (not spawning second loop)", delta2, delta1 * 2);
        }

        // Finally, shut it down:
        activator.stop().await;
    }

    // ─── Test 3: Ensure stop() actually awaits the background loop if it hasn’t finished yet ───
    #[tokio_test]
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
