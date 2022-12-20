/// Sero is a simple and lightweight library for maintaining a shared store of locks.
///
/// ## Basic Usage
///
/// ```rust
/// use sero::LockStore;
///
/// let store = LockStore::new();
///
/// // to lock asynchronously use
/// let guard = store.lock("key").await;
/// // to lock synchronously use
/// let guard = store.lock("key").wait();
/// // NOTE: synchronously locking will "park" the current thread until the lock is acquired
///
/// // locks are released when the LockGuard is dropped
/// // either with the drop function or when they go out of scope
/// drop(guard);
/// ```
use std::{
    collections::VecDeque,
    future::Future,
    sync::{Arc, Mutex},
    task::{Poll, Waker},
    thread,
};

use dashmap::{mapref::entry::Entry, DashMap};

/// A LockStore for sharing and managing a set of locks.
///
/// To lock a specific key with the store use `LockStore::lock(key)`, any subsequent calls for that key will remain locked until the current holder is released (the returned LockGuard is dropped).
/// When there are no longer any guards (when nothing is locking the lock) and there are no waiters left (no subsequent calls are waiting for the lock) then the lock will be placed into an unused locks queue and reused for a different key in the future.
///
/// To configure the number of locks kept in the queue, the `LockStore` can be created with `LockStore::with_custom_unused_locks`. The default value for `LockStore::new()` is 100 locks.
///
/// # Example
/// ```
/// use sero::LockStore;
///
/// let store = LockStore::with_custom_unused_locks(1000);
/// let waiter = store.lock("test");
/// let guard = waiter.wait();
/// // the lock is released here
/// drop(guard);
/// ```
#[derive(Clone, Debug)]
pub struct LockStore<K>
where
    K: std::hash::Hash + Eq + Clone + Send + Sync + 'static,
{
    locks: Arc<DashMap<K, Lock<K>>>,
    unused_locks: Arc<Mutex<VecDeque<Lock<K>>>>,
    keep_unused_locks: usize,
}

impl<K> LockStore<K>
where
    K: std::hash::Hash + Eq + Clone + Send + Sync + 'static,
{
    /// Create a new LockStore.
    ///
    /// # Example
    /// ```
    /// use sero::LockStore;
    ///
    /// let store = LockStore::new();
    ///
    /// let guard = store.lock("test").wait();
    /// ```
    #[inline(always)]
    pub fn new() -> Self {
        Self::with_custom_unused_locks(100)
    }

    /// Provide a custom number of unused locks to keep in the internal queue rather than recreating locks.
    ///
    /// The default value when this function is not used is 100 locks.
    ///
    /// # Example
    /// ```
    /// use sero::LockStore;
    ///
    /// // now the store will keep up to 1000 unused locks in the queue to prevent reallocating them.
    /// let store = LockStore::with_custom_unused_locks(1000);
    /// ```
    #[inline(always)]
    pub fn with_custom_unused_locks(keep_unused_locks: usize) -> Self {
        Self {
            locks: Arc::new(DashMap::new()),
            unused_locks: Arc::new(Mutex::new(VecDeque::new())),
            keep_unused_locks,
        }
    }

    /// Lock a specific key and get the relevant LockWaiter.
    /// To actually acquire the lock either use the wait() method to lock synchronously or .await to lock asynchronously.
    ///
    /// # Example
    /// ```
    /// // acquire a lock
    /// let guard = store.lock("test").wait();
    ///
    /// // to acquire the lock asynchronously use
    /// let guard = store.lock("test").await;
    ///
    /// // the lock is released here
    /// drop(guard);
    /// ```
    pub fn lock(&self, key: K) -> LockWaiter<K> {
        let lock = {
            let entry = self.locks.entry(key.clone());
            match &entry {
                Entry::Occupied(l) => l.get().clone(),
                Entry::Vacant(_) => {
                    let l = if let Some(l) = self.unused_locks.lock().unwrap().pop_front() {
                        l
                    } else {
                        Lock::new(self.clone())
                    };
                    entry.or_insert(l.clone());
                    l
                },
            }
        };
        lock.lock(key)
    }
}

impl<K> Default for LockStore<K>
where
    K: std::hash::Hash + Eq + Clone + Send + Sync + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug)]
struct Lock<K>
where
    K: std::hash::Hash + Eq + Clone + Send + Sync + 'static,
{
    state: Arc<Mutex<(bool, VecDeque<LockWaiter<K>>)>>,
    store: LockStore<K>,
}

impl<K> Lock<K>
where
    K: std::hash::Hash + Eq + Clone + Send + Sync + 'static,
{
    #[inline(always)]
    pub(crate) fn new(store: LockStore<K>) -> Self {
        Self {
            state: Arc::new(Mutex::new((false, VecDeque::new()))),
            store,
        }
    }

    pub(crate) fn lock(&self, key: K) -> LockWaiter<K> {
        let mut state = self.state.lock().unwrap();
        let waiter = LockWaiter::new(self.store.clone(), key);
        state.1.push_back(waiter.clone());
        if !state.0 {
            state.0 = true;
            let mut state = waiter.state.lock().unwrap();
            state.0 = true;
        }
        waiter
    }
}

/// A `LockWaiter` represents a waiter waiting to access a lock on a key.
///
/// The `LockWaiter` can be consumed synchronously using `LockWaiter::wait` (which will park the current thread until it is available) or asynchronously through `.await`.
///
/// # Example:
/// ```
/// let waiter = store.lock("test");
/// let guard = waiter.wait();
/// // OR
/// let guard = waiter.await;
/// ```
#[derive(Clone, Debug)]
pub struct LockWaiter<K>
where
    K: std::hash::Hash + Eq + Clone + Send + Sync + 'static,
{
    #[allow(clippy::type_complexity)]
    state: Arc<Mutex<(bool, Option<Waker>, Option<thread::Thread>)>>,
    store: LockStore<K>,
    key: K,
}

impl<K> LockWaiter<K>
where
    K: std::hash::Hash + Eq + Clone + Send + Sync + 'static,
{
    #[inline(always)]
    pub(crate) fn new(store: LockStore<K>, key: K) -> Self {
        Self {
            state: Arc::new(Mutex::new((false, None, None))),
            store,
            key,
        }
    }

    pub(crate) fn wake(&self) {
        let mut state = self.state.lock().unwrap();
        state.0 = true;
        if let Some(waker) = state.1.take() {
            waker.wake();
        }
        if let Some(thread) = state.2.take() {
            thread.unpark();
        }
    }

    /// Wait for the lock to be available and acquire a guard, when the guard is dropped the lock is released and can be acquired again
    /// **Note:** Calling this method will park the current thread, this may cause issues, especially in an asynchronous context, and cause a deadlock.
    ///
    /// # Example
    /// ```
    /// let waiter = store.lock("test");
    /// let guard = waiter.wait();
    pub fn wait(self) -> LockGuard<K>
    where
        K: std::hash::Hash + Eq + Clone + Send + Sync + 'static,
    {
        let mut state = self.state.lock().unwrap();
        state.2 = Some(thread::current());
        drop(state);
        loop {
            let state = self.state.lock().unwrap();
            if state.0 {
                break;
            }
            drop(state);
            thread::park();
        }
        LockGuard::new(self.store.clone(), self.key.clone())
    }

    pub(crate) fn completed(&self) -> bool {
        self.state.lock().unwrap().0
    }
}

impl<K> Future for LockWaiter<K>
where
    K: std::hash::Hash + Eq + Clone + Send + Sync + 'static,
{
    type Output = LockGuard<K>;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let mut state = self.state.lock().unwrap();
        if state.0 {
            Poll::Ready(LockGuard::new(self.store.clone(), self.key.clone()))
        } else {
            state.1 = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

/// Represents a hold on a specific lock.
///
/// The lock will be released and can be reacquired when the `LockGuard` is dropped.
///
/// # Example
/// ```
/// let guard = store.lock("test").wait();
/// // the lock is released here
/// drop(guard);
/// ```
#[derive(Debug)]
pub struct LockGuard<K>
where
    K: std::hash::Hash + Eq + Clone + Send + Sync + 'static,
{
    store: LockStore<K>,
    key: K,
}

impl<K> LockGuard<K>
where
    K: std::hash::Hash + Eq + Clone + Send + Sync + 'static,
{
    #[inline(always)]
    pub(crate) fn new(store: LockStore<K>, key: K) -> Self {
        Self { store, key }
    }
}

impl<K> Drop for LockGuard<K>
where
    K: std::hash::Hash + Eq + Clone + Send + Sync + 'static,
{
    /// Drops the `LockGuard` and releases the lock, allowing the next waiter to acquire it.
    ///
    /// # Example
    /// ```
    /// let guard = store.lock("test").wait()
    /// // drops the guard
    /// drop(guard);
    /// ```
    fn drop(&mut self) {
        {
            let lock = self.store.locks.get(&self.key).unwrap();
            let mut state = lock.value().state.lock().unwrap();
            state.0 = false;
            while let Some(waiter) = state.1.pop_front() {
                if waiter.completed() {
                    continue;
                }
                waiter.wake();
                return;
            }
        }
        let (_, lock) = self.store.locks.remove(&self.key).unwrap();
        let mut unused_locks = self.store.unused_locks.lock().unwrap();
        if unused_locks.len() < self.store.keep_unused_locks {
            unused_locks.push_back(lock)
        }
    }
}
