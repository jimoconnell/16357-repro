# Offloadable EP + Locks Reproducer Notes

## Summary

In testing, we observed that **when an EntryProcessor is marked Offloadable but the key is explicitly locked**, Hazelcast does **not** offload the work to the cached executor. Instead, it keeps the work on the **partition-operation threads**. This means the offloading path is bypassed as long as a lock is held on the entry.

This explains why `getAll` operations stall for roughly the EP processing time (for example ~2 seconds), even on a *different map* sharing the same partition layout.

## Evidence

Using the class **OffloadableLockOnlyReproducer**, the following output was produced:

```
Started member, keys=50000
Mode: Offloadable EP + explicit locks
EP sleep millis: 2000
Hot keys: 1000
getAll batch size: 1000
positionsMap name: positions
[positions] getAll latency 30590 µs, size=1000
[OffloadableEP] K0 on hz.peaceful_yalow.partition-operation.thread-8
[OffloadableEP] K1 on hz.peaceful_yalow.partition-operation.thread-5
[positions] getAll latency 1917364 µs, size=1000
[OffloadableEP] K2 on hz.peaceful_yalow.partition-operation.thread-1
[positions] getAll latency 1894792 µs, size=1000
[OffloadableEP] K3 on hz.peaceful_yalow.partition-operation.thread-7
[positions] getAll latency 1907360 µs, size=1000
```

### Why this matters

If the EP were truly offloaded, the thread names would look like:

```
hz.<cluster>.cached.thread-N
```

Instead, we consistently see:

```
hz.<cluster>.partition-operation.thread-N
```

This confirms that the EP's `process()` method is still running on the partition thread.

## Verification

In the same class, **commenting out the lock calls** (`positionsMap.lock(key)`) causes the thread output to switch to cached threads:

```
[OffloadableEP] K6 on hz.hardcore_hamilton.cached.thread-3
```

This proves the behavior:

* With a lock: EP executes on partition thread  
* Without a lock: EP executes on offloadable executor  

## Two-Map Validation

The class **OffloadableTwoMapLockReproducer** applies the same pattern across two maps (`positions` and `trades`). The result is identical:

### Trades map `getAll` stalls when Positions map uses lock + Offloadable EP

Even with two separate maps, when positions apply:

1. `lock(key)`  
2. `executeOnKey(offloadable EP)`  
3. `unlock(key)`

The EP still runs on partition-operation threads, and long-running work blocks the partition thread for **both maps**.

## Repository

Full reproducer code is available here:

<https://github.com/jimoconnell/16357-repro/>

It is public and contains **no customer-sensitive information**.

