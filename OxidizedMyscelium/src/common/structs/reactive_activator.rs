use std::fmt;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};

use syn::token::Mut;

pub struct ReactiveActivator {
    thread_handle: Arc<Mutex<Option<JoinHandle<()>>>>, // ← Tokio JoinHandle
    action: Arc<dyn Fn() + Send + Sync>,
    condition: Arc<dyn Fn() -> bool + Send + Sync>,
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
    pub fn new(action: Arc<dyn Fn() + Send + Sync>, condition: Arc<dyn Fn() -> bool + Send + Sync>) -> Arc<Self> {
        Arc::new(Self {
            thread_handle: Arc::new(Mutex::new(None)),
            action,
            condition,
        })
    }

    /// Starts the background task if not already running.
    pub async fn start(&self) {
        let mut handle_guard = self.thread_handle.lock().await;
        if handle_guard.is_none() {
            let action = Arc::clone(&self.action);
            let condition = Arc::clone(&self.condition);
            let thread_handle = Arc::clone(&self.thread_handle);

            println!("Starting the ReactiveActivator task...");

            // Spawn the async loop
            let join_handle = tokio::spawn(async move {
                loop {
                    println!("Executing action...");
                    (action)();

                    let cond = (condition)();
                    println!("Condition result: {:?}", cond);
                    if cond {
                        println!("Condition met, stopping loop.");
                        break;
                    }

                    // avoid busy-spin
                    sleep(Duration::from_millis(500)).await;
                }

                // Clear the handle when done
                let mut guard = thread_handle.lock().await;
                *guard = None;
            });

            *handle_guard = Some(join_handle);
        } else {
            println!("Task already started.");
        }
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
    use tokio::time::{sleep, Duration};

    // For clarity, alias the macro:
    use tokio::test as tokio_test;

    // ─── Test 1: Ensure the ReactiveActivator loop calls action exactly N times and then stops ───
    #[tokio_test]
    async fn reactive_activator_runs_exact_number_of_times_then_stops() {
        // We want the loop to run exactly 3 times, then exit.

        // Shared counter for action calls:
        let action_count = Arc::new(AtomicUsize::new(0));
        let action_count_clone = action_count.clone();
        let action_closure: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
            action_count_clone.fetch_add(1, Ordering::SeqCst);
        });

        // Shared counter for condition checks:
        // We'll return `false` for the first 2 checks; on the 3rd, return `true`.
        let condition_count = Arc::new(AtomicUsize::new(0));
        let condition_count_clone = condition_count.clone();
        let condition_closure: Arc<dyn Fn() -> bool + Send + Sync> = Arc::new(move || {
            // fetch_add returns the old value; so on the third call, old value == 2
            let prev = condition_count_clone.fetch_add(1, Ordering::SeqCst);
            prev >= 2
        });

        // Build the activator:
        let activator = ReactiveActivator::new(action_closure, condition_closure);

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

        // Shared counter for action calls:
        let action_count = Arc::new(AtomicUsize::new(0));
        let action_count_clone = action_count.clone();
        let action_closure: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
            action_count_clone.fetch_add(1, Ordering::SeqCst);
        });

        // Condition closure that never returns true (so the loop keeps running):
        let condition_closure: Arc<dyn Fn() -> bool + Send + Sync> = Arc::new(move || false);

        let activator = ReactiveActivator::new(action_closure, condition_closure);

        // 1) First start:
        activator.start().await;

        // Let it run for 600ms (so it can do roughly 1 iteration—remember each loop has a 500ms sleep).
        sleep(Duration::from_millis(600)).await;
        let count_after_first_window = action_count.load(Ordering::SeqCst);

        // 2) Call start() again while it’s still running.
        activator.start().await;

        // Let it run another 600ms:
        sleep(Duration::from_millis(600)).await;
        let count_after_second_window = action_count.load(Ordering::SeqCst);

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

        let action_count = Arc::new(AtomicUsize::new(0));
        let action_count_clone = action_count.clone();
        let action_closure: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
            action_count_clone.fetch_add(1, Ordering::SeqCst);
        });

        // Condition that returns false for a little while, then true
        // We’ll use a shared AtomicBool and set it to true from the test after 300ms.
        let cond_flag = Arc::new(AtomicBool::new(false));
        let cond_flag_clone = cond_flag.clone();
        let condition_closure: Arc<dyn Fn() -> bool + Send + Sync> = Arc::new(move || cond_flag_clone.load(Ordering::SeqCst));

        let activator = ReactiveActivator::new(action_closure, condition_closure);

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
}
