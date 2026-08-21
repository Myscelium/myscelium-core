// SPDX-License-Identifier: MPL-2.0
// Copyright © 2021-2026 Cristian Camargo Filho

// use rand::Rng;
// use std::sync::Arc;
// use std::thread;
// use tokio::sync::Mutex;

// use parking_lot::lock_api::RawMutex;
// use std::sync::Mutex as StdMutex;
// use std::time::{Duration, Instant};

// use std::sync::MutexGuard as StdMutexGuard;

// // // Define a trait that abstracts the locking behavior.
// // trait TryLock<'a> {
// //     type Guard;

// //     fn try_lock(&'a self) -> Result<Self::Guard, ()>;
// // }

// // impl<'a, T> TryLock<'a> for Arc<StdMutex<T>> {
// //     type Guard = StdMutexGuard<'a, T>;

// //     fn try_lock(&'a self) -> Result<Self::Guard, ()> {
// //         self.try_lock().map_err(|_| ())
// //     }
// // }

// // impl<'a, T> TryLock<'a> for Arc<LockApiMutex<RawMutex, T>> {
// //     type Guard = MutexGuard<'a, RawMutex, T>;

// //     fn try_lock(&'a self) -> Result<Self::Guard, ()> {
// //         self.try_lock().map_err(|_| ())
// //     }
// // }

// use parking_lot::{Mutex as ParkingLotMutex, MutexGuard as ParkingLotMutexGuard};
// use rand::thread_rng;

// /// Attempts to acquire a lock and execute a closure, retrying with a randomized delay if the lock is not immediately available.
// ///
// /// # Arguments
// /// * `mutex` - A reference to the Arc containing the Mutex.
// /// * `f` - A closure that will be executed once the lock is acquired.
// pub fn smart_lock<T, F>(mutex: &Arc<Mutex<T>>, f: F)
// where
//     F: FnOnce(&mut T),
// {
//     let mut rng = rand::thread_rng();
//     let start_time = Instant::now();
//     let timeout = Duration::from_secs(10); // Example timeout of 10 seconds

//     loop {
//         match mutex.try_lock() {
//             Ok(mut guard) => {
//                 f(&mut guard);
//                 return;
//             },
//             Err(_) => {
//                 if start_time.elapsed() > timeout {
//                     eprintln!("Failed to acquire lock after {:?}, giving up", timeout);
//                     return;
//                 }
//                 let sleep_duration = Duration::from_millis(10) + Duration::from_millis(rng.gen_range(0..10));
//                 thread::sleep(sleep_duration);
//             },
//         }
//     }
// }

// // // Define a trait for try_lock behavior with lifetime 'a
// // trait TryLockable<'a> {
// //     type Guard: 'a;

// //     fn try_lock(&self) -> Option<Self::Guard>;
// // }

// // // Implement TryLockable for std::sync::Mutex
// // impl<'a, T> TryLockable<'a> for StdMutex<T> {
// //     type Guard = StdMutexGuard<'_, T>; // MutexGuard has its own scoped lifetime

// //     fn try_lock(&self) -> Option<Self::Guard> {
// //         self.try_lock().ok()
// //     }
// // }

// // // Implement TryLockable for parking_lot::Mutex
// // impl<'a, T> TryLockable<'a> for ParkingLotMutex<T> {
// //     type Guard = ParkingLotMutexGuard<'_, T>; // MutexGuard has its own scoped lifetime

// //     fn try_lock(&self) -> Option<Self::Guard> {
// //         self.try_lock()
// //     }
// // }

// // // Now the smart_lock function can be generic over the TryLockable trait
// // pub fn smart_lock<'a, T, F, M>(mutex: &'a Arc<M>, f: F)
// // where
// //     M: TryLockable<'a, Guard = T>,
// //     F: FnOnce(&mut T),
// // {
// //     let mut rng = thread_rng();
// //     let start_time = Instant::now();
// //     let timeout = Duration::from_secs(10); // Example timeout of 10 seconds

// //     loop {
// //         match mutex.try_lock() {
// //             Some(mut guard) => {
// //                 f(&mut guard);
// //                 return;
// //             },
// //             None => {
// //                 if start_time.elapsed() > timeout {
// //                     eprintln!("Failed to acquire lock after {:?}, giving up", timeout);
// //                     return;
// //                 }
// //                 let sleep_duration = Duration::from_millis(10) + Duration::from_millis(rng.gen_range(0..10));
// //                 thread::sleep(sleep_duration);
// //             },
// //         }
// //     }
// // }
