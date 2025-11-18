**Offloadable EP + no locks on Positions:**

```bash
mvn -q exec:java \
  -Dexec.mainClass=org.example.TwoMapLockEpReproducer \
  -Dexec.args="--offloadable --no-locks"
```

**Fast**:

```
Started member, keys=50000
Using offloadable EP: true
Locks only (no EP): false
No locks: true
EP sleep millis: 2000
Hot keys: 1000
getAll batch size: 1000
tradesMap name: trades
positionsMap name: positions
[OffloadableEP] K0 on hz.admiring_nobel.cached.thread-3
[trades] getAll latency 29791 µs, size=1000
[trades] getAll latency 20339 µs, size=1000
[trades] getAll latency 19075 µs, size=1000
[trades] getAll latency 2274 µs, size=1000
[trades] getAll latency 11382 µs, size=1000
[trades] getAll latency 2638 µs, size=1000
[trades] getAll latency 3558 µs, size=1000
[trades] getAll latency 2219 µs, size=1000
[trades] getAll latency 4362 µs, size=1000
[trades] getAll latency 4696 µs, size=1000
[trades] getAll latency 7090 µs, size=1000
[trades] getAll latency 6240 µs, size=1000
[trades] getAll latency 5235 µs, size=1000
[trades] getAll latency 5598 µs, size=1000
[trades] getAll latency 4316 µs, size=1000
[trades] getAll latency 4808 µs, size=1000
[trades] getAll latency 5346 µs, size=1000
[trades] getAll latency 2557 µs, size=1000
[trades] getAll latency 5003 µs, size=1000
[OffloadableEP] K1 on hz.admiring_nobel.cached.thread-3
```



**Non offloadable EP + locks on Positions**

```bash
mvn -q exec:java  -Dexec.mainClass=org.example.TwoMapLockEpReproducer   	
```

**Slow**:	

```
Started member, keys=50000
Using offloadable EP: false
Locks only (no EP): false
No locks: false
EP sleep millis: 2000
Hot keys: 1000
getAll batch size: 1000
tradesMap name: trades
positionsMap name: positions
[trades] getAll latency 23939 µs, size=1000
[EP] K0 on hz.determined_kirch.partition-operation.thread-8
[EP] K1 on hz.determined_kirch.partition-operation.thread-5
[trades] getAll latency 1916926 µs, size=1000
[EP] K2 on hz.determined_kirch.partition-operation.thread-1
[trades] getAll latency 1900109 µs, size=1000
[EP] K3 on hz.determined_kirch.partition-operation.thread-7
[trades] getAll latency 1899662 µs, size=1000
[EP] K4 on hz.determined_kirch.partition-operation.thread-6
[trades] getAll latency 1899982 µs, size=1000
[EP] K5 on hz.determined_kirch.partition-operation.thread-2
[trades] getAll latency 1900776 µs, size=1000
[EP] K6 on hz.determined_kirch.partition-operation.thread-5
[trades] getAll latency 1902129 µs, size=1000
```



**Offloadable EP + locks on Positions:**

```bash
mvn -q exec:java \
  -Dexec.mainClass=org.example.TwoMapLockEpReproducer \
  -Dexec.args="--offloadable"
```

Slow:

```
Started member, keys=50000
Using offloadable EP: true
Locks only (no EP): false
No locks: false
EP sleep millis: 2000
Hot keys: 1000
getAll batch size: 1000
tradesMap name: trades
positionsMap name: positions
[trades] getAll latency 25090 µs, size=1000
[OffloadableEP] K0 on hz.practical_almeida.partition-operation.thread-8
[OffloadableEP] K1 on hz.practical_almeida.partition-operation.thread-5
[trades] getAll latency 1917398 µs, size=1000
[OffloadableEP] K2 on hz.practical_almeida.partition-operation.thread-1
[trades] getAll latency 1894563 µs, size=1000
[OffloadableEP] K3 on hz.practical_almeida.partition-operation.thread-7
[trades] getAll latency 1907258 µs, size=1000
[OffloadableEP] K4 on hz.practical_almeida.partition-operation.thread-6
[trades] getAll latency 1897971 µs, size=1000
[OffloadableEP] K5 on hz.practical_almeida.partition-operation.thread-2
[trades] getAll latency 1900806 µs, size=1000
[OffloadableEP] K6 on hz.practical_almeida.partition-operation.thread-5
[trades] getAll latency 1895112 µs, size=1000
[OffloadableEP] K7 on hz.practical_almeida.partition-operation.thread-5
[OffloadableEP] K8 on hz.practical_almeida.partition-operation.thread-3
[trades] getAll latency 3903485 µs, size=1000
[OffloadableEP] K9 on hz.practical_almeida.partition-operation.thread-8
[trades] getAll latency 1899558 µs, size=1000
[OffloadableEP] K10 on hz.practical_almeida.partition-operation.thread-5
```



**Non offloadable EP, no locks:**

```bash
mvn -q exec:java \
  -Dexec.mainClass=org.example.TwoMapLockEpReproducer \
  -Dexec.args="--no-locks"	
```

Slow:

```
Started member, keys=50000
Using offloadable EP: false
Locks only (no EP): false
No locks: true
EP sleep millis: 2000
Hot keys: 1000
getAll batch size: 1000
tradesMap name: trades
positionsMap name: positions
[EP] K0 on hz.hungry_napier.partition-operation.thread-8
[EP] K1 on hz.hungry_napier.partition-operation.thread-5
[trades] getAll latency 2025827 µs, size=1000
[EP] K2 on hz.hungry_napier.partition-operation.thread-1
[trades] getAll latency 1894606 µs, size=1000
[EP] K3 on hz.hungry_napier.partition-operation.thread-7
[trades] getAll latency 1900516 µs, size=1000
[EP] K4 on hz.hungry_napier.partition-operation.thread-6
[trades] getAll latency 1895525 µs, size=1000
[EP] K5 on hz.hungry_napier.partition-operation.thread-2
[trades] getAll latency 1904646 µs, size=1000
[EP] K6 on hz.hungry_napier.partition-operation.thread-5
[trades] getAll latency 1901740 µs, size=1000
```

**Here's what it shows, (I think)**

1. Offloadable EP + no locks on Positions, Trades getAll is fast

   EP runs on hz.*cached.thread-*, getAll on trades stays in the low millisecond range even though the EP sleeps 2 seconds.

   So: Offloadable EP by itself is fine, no stalls on trades.

2. Non offloadable EP + locks on Positions, Trades getAll is slow

   EP runs on hz.*partition-operation.thread-*, getAll latency on trades is ~1.9 to ~2 million microseconds.

   So: long running EP on partition threads, with locks, clearly stalls operations on the other map.

3. Offloadable EP + locks on Positions, Trades getAll is still slow

   Key detail: your log shows the Offloadable EP is running on hz.practical_almeida.partition-operation.thread-*, not on hz.*cached.thread-*.

   So in this pattern, even though the class implements Offloadable, the work is still actually happening on the partition-operation threads while the keys are locked. The result looks just like non offloadable: trades getAll in the 1,9–3,9 second range.

4. Non offloadable EP, no locks on Positions, Trades getAll is slow

   EP is on partition-operation threads, no explicit locks at all, and trades getAll is still around 1,9–2,0 seconds.

   So: a long running EP on partition threads is enough by itself to stall trades, even without a map.lock.



**The class I used:**

```java
package org.example;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Offloadable;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.IMap;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class TwoMapLockEpReproducer {

    private static final String TRADES_MAP = "trades";
    private static final String POSITIONS_MAP = "positions";

    public static void main(String[] args) throws Exception {

        boolean useOffloadable = Arrays.asList(args).contains("--offloadable");
        boolean locksOnly = Arrays.asList(args).contains("--locks-only");
        boolean noLocks = Arrays.asList(args).contains("--no-locks");

        long epSleepMillis = 2000;
        int hotKeyCount = 1000;
        int getAllBatchSize = 1000;

        Config cfg = new Config();
        cfg.setClusterName("two-map-reproducer");

        String license = System.getenv("HZ_LICENSEKEY");
        if (license == null || license.isEmpty()) {
            System.err.println("WARNING: HZ_LICENSEKEY not set, cluster may start in OSS mode.");
        } else {
            cfg.setLicenseKey(license);
        }

        cfg.addMapConfig(new MapConfig(TRADES_MAP));
        cfg.addMapConfig(new MapConfig(POSITIONS_MAP));

        HazelcastInstance hz = Hazelcast.newHazelcastInstance(cfg);

        IMap<String, Long> tradesMap = hz.getMap(TRADES_MAP);
        IMap<String, Long> positionsMap = hz.getMap(POSITIONS_MAP);

        List<String> allKeys = new ArrayList<>();
        for (int i = 0; i < 50_000; i++) {
            String key = "K" + i;
            allKeys.add(key);
            tradesMap.put(key, 0L);
            positionsMap.put(key, 0L);
        }

        if (hotKeyCount > allKeys.size()) hotKeyCount = allKeys.size();
        if (getAllBatchSize > allKeys.size()) getAllBatchSize = allKeys.size();

        List<String> hotKeys = allKeys.subList(0, hotKeyCount);
        List<String> getAllKeys = allKeys.subList(0, getAllBatchSize);

        System.out.println("Started member, keys=" + allKeys.size());
        System.out.println("Using offloadable EP: " + useOffloadable);
        System.out.println("Locks only (no EP): " + locksOnly);
        System.out.println("No locks: " + noLocks);
        System.out.println("EP sleep millis: " + epSleepMillis);
        System.out.println("Hot keys: " + hotKeys.size());
        System.out.println("getAll batch size: " + getAllKeys.size());
        System.out.println("tradesMap name: " + TRADES_MAP);
        System.out.println("positionsMap name: " + POSITIONS_MAP);

        Thread positionsThread = new Thread(
                () -> runPositionsLockEpLoop(
                        positionsMap,
                        hotKeys,
                        epSleepMillis,
                        useOffloadable,
                        locksOnly,
                        noLocks
                ),
                "positions-ep-thread"
        );
        positionsThread.setDaemon(true);
        positionsThread.start();

        runTradesGetAllDriver(tradesMap, getAllKeys);

        hz.shutdown();
    }

    private static void runPositionsLockEpLoop(IMap<String, Long> positionsMap,
                                               List<String> keys,
                                               long sleepMillis,
                                               boolean useOffloadable,
                                               boolean locksOnly,
                                               boolean noLocks) {
        while (true) {
            long txStart = System.nanoTime();
            try {

                if (!noLocks) {
                    for (String key : keys) {
                        positionsMap.lock(key);
                    }
                }

                if (locksOnly) {
                    try {
                        Thread.sleep(sleepMillis);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                } else {
                    for (String key : keys) {
                        EntryProcessor<String, Long, Void> ep =
                                useOffloadable
                                        ? new OffloadableSleepEP(sleepMillis)
                                        : new SleepEP(sleepMillis);
                        positionsMap.executeOnKey(key, ep);
                    }
                }

            } finally {
                if (!noLocks) {
                    for (String key : keys) {
                        try {
                            positionsMap.unlock(key);
                        } catch (Exception ignore) {
                        }
                    }
                }
            }

            long txMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - txStart);
            System.out.println(
                    "[positions] TX finished in " + txMs +
                    " ms, sleep=" + sleepMillis +
                    " ms, locksOnly=" + locksOnly +
                    ", offloadable=" + useOffloadable +
                    ", noLocks=" + noLocks
            );
        }
    }

    private static void runTradesGetAllDriver(IMap<String, Long> tradesMap,
                                              List<String> keys) throws InterruptedException {
        Random rnd = new Random();

        while (true) {
            List<String> batch = new ArrayList<>(keys);
            Collections.shuffle(batch, rnd);

            long start = System.nanoTime();
            Map<String, Long> result = tradesMap.getAll(new HashSet<>(batch));
            long durationMicros = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - start);

            System.out.printf(Locale.ROOT,
                    "[trades] getAll latency %d µs, size=%d%n",
                    durationMicros,
                    result.size()
            );

            Thread.sleep(100);
        }
    }

    public static class SleepEP implements EntryProcessor<String, Long, Void>, Serializable {

        private final long sleepMillis;

        public SleepEP(long sleepMillis) {
            this.sleepMillis = sleepMillis;
        }

        @Override
        public Void process(Map.Entry<String, Long> entry) {
            try {
                System.out.println("[EP] " + entry.getKey()
                        + " on " + Thread.currentThread().getName());
                Thread.sleep(sleepMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return null;
        }
    }

    public static class OffloadableSleepEP
            implements EntryProcessor<String, Long, Void>, Offloadable, Serializable {

        private final long sleepMillis;

        public OffloadableSleepEP(long sleepMillis) {
            this.sleepMillis = sleepMillis;
        }

        @Override
        public String getExecutorName() {
            return Offloadable.OFFLOADABLE_EXECUTOR;
        }

        @Override
        public Void process(Map.Entry<String, Long> entry) {
            try {
                System.out.println("[OffloadableEP] " + entry.getKey()
                        + " on " + Thread.currentThread().getName());
                Thread.sleep(sleepMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return null;
        }
    }
}
```

