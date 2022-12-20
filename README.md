# sero

Sero is a simple and lightweight library for maintaining a shared store of locks.

## Basic Usage

```rust
use sero::LockStore;

let store = LockStore::new();

// to lock asynchronously use
let guard = store.lock("key").await;
// to lock synchronously use
let guard = store.lock("key").wait();
// NOTE: synchronously locking will "park" the current thread until the lock is acquired

// locks are released when the LockGuard is dropped
// either with the drop function or when they go out of scope
drop(guard);
```
