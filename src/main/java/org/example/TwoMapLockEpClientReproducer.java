package org.example;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.Offloadable;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.IMap;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class TwoMapLockEpClientReproducer {

    private static final String TRADES_MAP = "trades";
    private static final String POSITIONS_MAP = "positions";

    public static void main(String[] args) throws Exception {
        String serverAddress = args.length > 0 && !args[0].startsWith("--") 
                ? args[0] 
                : "localhost:5701";
        boolean useOffloadable = Arrays.asList(args).contains("--offloadable");
        
        long epSleepMillis = 2000;
        int hotKeyCount = 1000;
        int getAllBatchSize = 1000;
        int testDurationSeconds = 50;

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClusterName("dev");
        clientConfig.getNetworkConfig().addAddress(serverAddress);

        System.out.println("========================================");
        System.out.println("  Offloadable EP + Locks Test");
        System.out.println("========================================");
        System.out.println("Connecting to server: " + serverAddress);
        System.out.println("Cluster: dev");
        System.out.println("EP sleep: " + epSleepMillis + "ms");
        System.out.println("Test duration: " + testDurationSeconds + " seconds per test");
        System.out.println();

        HazelcastInstance hz = HazelcastClient.newHazelcastClient(clientConfig);
        System.out.println("✓ Connected to cluster");
        System.out.println();

        IMap<String, Long> tradesMap = hz.getMap(TRADES_MAP);
        IMap<String, Long> positionsMap = hz.getMap(POSITIONS_MAP);

        List<String> allKeys = new ArrayList<>();
        for (int i = 0; i < 50_000; i++) {
            allKeys.add("K" + i);
        }

        if (hotKeyCount > allKeys.size())
            hotKeyCount = allKeys.size();
        if (getAllBatchSize > allKeys.size())
            getAllBatchSize = allKeys.size();

        List<String> hotKeys = allKeys.subList(0, hotKeyCount);
        List<String> getAllKeys = allKeys.subList(0, getAllBatchSize);

        if (useOffloadable) {
            // Test 1: WITH locks
            System.out.println("========================================");
            System.out.println("  TEST 1: Offloadable EP WITH LOCKS");
            System.out.println("========================================");
            System.out.println("Current behavior (BUG or by Design?):");
            System.out.println("  - EP runs on partition-operation threads (NOT offloaded)");
            System.out.println("  - getAll latency spikes to ~" + epSleepMillis + "ms");
            System.out.println("  - Locks prevent offloading even though EP is marked Offloadable");
            System.out.println();
            System.out.println("Expected behavior (if bug is fixed):");
            System.out.println("  - EP should run on cached threads (OFFLOADED)");
            System.out.println("  - getAll latency should stay low");
            System.out.println("  - Watch server logs for thread names");
            System.out.println();
            
            runTest(hz, tradesMap, positionsMap, hotKeys, getAllKeys, 
                    epSleepMillis, useOffloadable, false, testDurationSeconds);
            
            Thread.sleep(2000); // Brief pause between tests
            
            // Test 2: WITHOUT locks
            System.out.println();
            System.out.println("========================================");
            System.out.println("  TEST 2: Offloadable EP WITHOUT LOCKS");
            System.out.println("========================================");
            System.out.println("Expected behavior (works correctly):");
            System.out.println("  - EP runs on cached threads (OFFLOADED)");
            System.out.println("  - getAll latency stays low");
            System.out.println("  - This demonstrates how offloading SHOULD work");
            System.out.println("  - Watch server logs for thread names");
            System.out.println();
            
            runTest(hz, tradesMap, positionsMap, hotKeys, getAllKeys, 
                    epSleepMillis, useOffloadable, true, testDurationSeconds);
            
            Thread.sleep(2000); // Brief pause between tests
            
            // Test 3: ExecutorService WITH locks
            System.out.println();
            System.out.println("========================================");
            System.out.println("  TEST 3: ExecutorService WITH LOCKS");
            System.out.println("========================================");
            System.out.println("Expected behavior (alternative solution):");
            System.out.println("  - Task runs on executor threads (NOT partition threads)");
            System.out.println("  - getAll latency should stay low");
            System.out.println("  - This demonstrates ExecutorService as alternative to EP");
            System.out.println("  - Watch server logs for thread names");
            System.out.println();
            
            runExecutorServiceTest(hz, tradesMap, positionsMap, hotKeys, getAllKeys, 
                    epSleepMillis, testDurationSeconds);
        } else {
            System.out.println("ERROR: Must use --offloadable flag");
            System.out.println("Usage: java ... TwoMapLockEpClientReproducer [server] --offloadable");
            System.exit(1);
        }

        System.out.println();
        System.out.println("========================================");
        System.out.println("  All Tests Complete");
        System.out.println("========================================");
        
        // Shutdown client gracefully
        try {
            hz.shutdown();
        } catch (Exception e) {
            // Ignore shutdown exceptions - they're expected when threads are still finishing
            System.err.println("Note: Some operations may have been interrupted during shutdown");
        }
    }
    
    private static void runTest(HazelcastInstance hz, IMap<String, Long> tradesMap, 
                                IMap<String, Long> positionsMap, List<String> hotKeys,
                                List<String> getAllKeys, long epSleepMillis, 
                                boolean useOffloadable, boolean noLocks, int durationSeconds) throws Exception {
        
        AtomicBoolean running = new AtomicBoolean(true);
        AtomicLong getAllCount = new AtomicLong(0);
        AtomicLong getAllTotalMicros = new AtomicLong(0);
        AtomicLong maxGetAllLatency = new AtomicLong(0);
        AtomicLong minGetAllLatency = new AtomicLong(Long.MAX_VALUE);
        
        Thread positionsThread = new Thread(
                () -> runPositionsLockEpLoop(
                        positionsMap, hotKeys, epSleepMillis, 
                        useOffloadable, false, noLocks, running),
                "positions-ep-thread");
        positionsThread.setDaemon(true);
        positionsThread.start();

        Thread getAllThread = new Thread(
                () -> {
                    try {
                        runTradesGetAllDriver(tradesMap, getAllKeys, running, 
                                getAllCount, getAllTotalMicros, maxGetAllLatency, minGetAllLatency);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                },
                "getAll-thread");
        getAllThread.setDaemon(true);
        getAllThread.start();

        // Run for specified duration
        long startTime = System.currentTimeMillis();
        long endTime = startTime + (durationSeconds * 1000);
        
        while (System.currentTimeMillis() < endTime) {
            Thread.sleep(1000);
            long elapsed = (System.currentTimeMillis() - startTime) / 1000;
            System.out.print("\r⏱  Running... " + elapsed + "s / " + durationSeconds + "s");
        }
        System.out.println();
        
        running.set(false);
        
        // Wait for threads to finish gracefully
        System.out.println("   Stopping background threads...");
        try {
            positionsThread.join(2000); // Wait up to 2 seconds
            getAllThread.join(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        // Print summary
        long count = getAllCount.get();
        if (count > 0) {
            long avgLatency = getAllTotalMicros.get() / count;
            long min = minGetAllLatency.get() == Long.MAX_VALUE ? 0 : minGetAllLatency.get();
            System.out.println();
            System.out.println("📊 Results Summary:");
            System.out.println("   getAll operations: " + count);
            System.out.println("   Average latency: " + avgLatency + " µs (" + (avgLatency / 1000.0) + " ms)");
            System.out.println("   Min latency: " + min + " µs (" + (min / 1000.0) + " ms)");
            System.out.println("   Max latency: " + maxGetAllLatency.get() + " µs (" + (maxGetAllLatency.get() / 1000.0) + " ms)");
        }
    }

    private static void runExecutorServiceTest(HazelcastInstance hz, 
                                               IMap<String, Long> tradesMap, 
                                               IMap<String, Long> positionsMap, 
                                               List<String> hotKeys,
                                               List<String> getAllKeys, 
                                               long workDurationMillis, 
                                               int durationSeconds) throws Exception {
        
        AtomicBoolean running = new AtomicBoolean(true);
        AtomicLong getAllCount = new AtomicLong(0);
        AtomicLong getAllTotalMicros = new AtomicLong(0);
        AtomicLong maxGetAllLatency = new AtomicLong(0);
        AtomicLong minGetAllLatency = new AtomicLong(Long.MAX_VALUE);
        
        IExecutorService executorService = hz.getExecutorService("default");
        
        Thread positionsThread = new Thread(
                () -> runPositionsExecutorServiceLoop(
                        positionsMap, executorService, hotKeys, workDurationMillis, running),
                "positions-executor-thread");
        positionsThread.setDaemon(true);
        positionsThread.start();

        Thread getAllThread = new Thread(
                () -> {
                    try {
                        runTradesGetAllDriver(tradesMap, getAllKeys, running, 
                                getAllCount, getAllTotalMicros, maxGetAllLatency, minGetAllLatency);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                },
                "getAll-thread");
        getAllThread.setDaemon(true);
        getAllThread.start();

        // Run for specified duration
        long startTime = System.currentTimeMillis();
        long endTime = startTime + (durationSeconds * 1000);
        
        while (System.currentTimeMillis() < endTime) {
            Thread.sleep(1000);
            long elapsed = (System.currentTimeMillis() - startTime) / 1000;
            System.out.print("\r⏱  Running... " + elapsed + "s / " + durationSeconds + "s");
        }
        System.out.println();
        
        running.set(false);
        
        // Wait for threads to finish gracefully
        System.out.println("   Stopping background threads...");
        try {
            positionsThread.join(2000);
            getAllThread.join(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        // Print summary
        long count = getAllCount.get();
        if (count > 0) {
            long avgLatency = getAllTotalMicros.get() / count;
            long min = minGetAllLatency.get() == Long.MAX_VALUE ? 0 : minGetAllLatency.get();
            System.out.println();
            System.out.println("📊 Results Summary:");
            System.out.println("   getAll operations: " + count);
            System.out.println("   Average latency: " + avgLatency + " µs (" + (avgLatency / 1000.0) + " ms)");
            System.out.println("   Min latency: " + min + " µs (" + (min / 1000.0) + " ms)");
            System.out.println("   Max latency: " + maxGetAllLatency.get() + " µs (" + (maxGetAllLatency.get() / 1000.0) + " ms)");
        }
    }

    private static void runPositionsExecutorServiceLoop(IMap<String, Long> positionsMap,
                                                        IExecutorService executorService,
                                                        List<String> keys,
                                                        long workDurationMillis,
                                                        AtomicBoolean running) {
        while (running.get()) {
            try {
                // Lock all keys
                for (String key : keys) {
                    if (!running.get()) break;
                    try {
                        positionsMap.lock(key);
                    } catch (Exception e) {
                        if (!running.get()) break;
                        throw e;
                    }
                }

                // Submit task to ExecutorService (runs on executor thread, NOT partition thread)
                Future<Void> future = executorService.submit(
                    new ProcessPositionsTask(new HashSet<>(keys), workDurationMillis, POSITIONS_MAP)
                );
                
                // Wait for completion
                try {
                    future.get();
                } catch (Exception e) {
                    if (!running.get()) break;
                    // Continue on other exceptions
                }

            } finally {
                // Always unlock
                for (String key : keys) {
                    try {
                        positionsMap.unlock(key);
                    } catch (Exception ignore) {
                        // Ignore unlock exceptions during shutdown
                    }
                }
            }

            if (!running.get()) break;
        }
    }

    private static void runPositionsLockEpLoop(IMap<String, Long> positionsMap,
            List<String> keys,
            long sleepMillis,
            boolean useOffloadable,
            boolean locksOnly,
            boolean noLocks,
            AtomicBoolean running) {
        while (running.get()) {
            try {
                if (!noLocks) {
                    for (String key : keys) {
                        if (!running.get()) break;
                        try {
                            positionsMap.lock(key);
                        } catch (Exception e) {
                            if (!running.get()) break; // Expected during shutdown
                            throw e;
                        }
                    }
                }

                if (locksOnly) {
                    try {
                        Thread.sleep(sleepMillis);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                } else {
                    for (String key : keys) {
                        if (!running.get()) break;
                        try {
                            EntryProcessor<String, Long, Void> ep = useOffloadable
                                    ? new OffloadableSleepEP(sleepMillis)
                                    : new SleepEP(sleepMillis);
                            positionsMap.executeOnKey(key, ep);
                        } catch (Exception e) {
                            if (!running.get()) break; // Expected during shutdown
                            // Continue with next key on other exceptions
                        }
                    }
                }

            } finally {
                if (!noLocks) {
                    for (String key : keys) {
                        try {
                            positionsMap.unlock(key);
                        } catch (Exception ignore) {
                            // Ignore unlock exceptions during shutdown
                        }
                    }
                }
            }

            if (!running.get()) break;
        }
    }

    private static void runTradesGetAllDriver(IMap<String, Long> tradesMap,
            List<String> keys, AtomicBoolean running,
            AtomicLong getAllCount, AtomicLong getAllTotalMicros,
            AtomicLong maxGetAllLatency, AtomicLong minGetAllLatency) throws InterruptedException {
        Random rnd = new Random();

        while (running.get()) {
            try {
                List<String> batch = new ArrayList<>(keys);
                Collections.shuffle(batch, rnd);

                long start = System.nanoTime();
                Map<String, Long> result = tradesMap.getAll(new HashSet<>(batch));
                long durationMicros = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - start);

                getAllCount.incrementAndGet();
                getAllTotalMicros.addAndGet(durationMicros);
                long currentMax = maxGetAllLatency.get();
                while (durationMicros > currentMax && !maxGetAllLatency.compareAndSet(currentMax, durationMicros)) {
                    currentMax = maxGetAllLatency.get();
                }
                long currentMin = minGetAllLatency.get();
                while (durationMicros < currentMin && !minGetAllLatency.compareAndSet(currentMin, durationMicros)) {
                    currentMin = minGetAllLatency.get();
                }

                // Print every 10th operation to show progress
                if (getAllCount.get() % 10 == 0) {
                    System.out.printf(Locale.ROOT,
                            "\r   [trades] getAll #%d: %d µs (%.1f ms), size=%d",
                            getAllCount.get(), durationMicros, durationMicros / 1000.0, result.size());
                }

                Thread.sleep(100);
            } catch (Exception e) {
                if (!running.get()) {
                    // Expected during shutdown
                    break;
                }
                // Log other exceptions but continue
                System.err.println("Error in getAll: " + e.getMessage());
            }
        }
    }

    public static class SleepEP implements EntryProcessor<String, Long, Void>, Serializable {

        private final long sleepMillis;

        public SleepEP(long sleepMillis) {
            this.sleepMillis = sleepMillis;
        }

        private static volatile int epCount = 0;
        
        @Override
        public Void process(Map.Entry<String, Long> entry) {
            try {
                int count = ++epCount;
                // Print more frequently - first 200, then every 5th
                boolean shouldPrint = count <= 200 || count % 5 == 0;
                
                if (shouldPrint) {
                    String msg = "[EP #" + count + "] " + entry.getKey()
                            + " on " + Thread.currentThread().getName();
                    System.out.println(msg);
                    System.err.println(msg);
                    System.out.flush();
                    System.err.flush();
                }
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

        private static volatile int epCount = 0;
        
        @Override
        public Void process(Map.Entry<String, Long> entry) {
            try {
                int count = ++epCount;
                String threadName = Thread.currentThread().getName();
                String status = threadName.contains("cached") ? "OFFLOADED" : "NOT OFFLOADED";
                
                // Print more frequently - first 200, then every 5th
                boolean shouldPrint = count <= 200 || count % 5 == 0;
                
                if (shouldPrint) {
                    // Print to both stdout and stderr for visibility
                    String msg = "[OffloadableEP #" + count + "] " + entry.getKey()
                            + " on " + threadName + " [" + status + "]";
                    System.out.println(msg);
                    System.err.println(msg);
                    System.out.flush();
                    System.err.flush();
                }
                Thread.sleep(sleepMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return null;
        }
    }

    /**
     * Task that runs on ExecutorService thread (NOT partition thread).
     * This allows long-running work without blocking partition threads.
     */
    public static class ProcessPositionsTask implements Callable<Void>, Serializable {
        
        private final Set<String> keys;
        private final long workDurationMillis;
        private final String mapName;
        
        private static volatile int taskCount = 0;
        
        public ProcessPositionsTask(Set<String> keys, long workDurationMillis, String mapName) {
            this.keys = keys;
            this.workDurationMillis = workDurationMillis;
            this.mapName = mapName;
        }
        
        @Override
        public Void call() throws Exception {
            int count = ++taskCount;
            String threadName = Thread.currentThread().getName();
            
            // Print first 200, then every 5th
            boolean shouldPrint = count <= 200 || count % 5 == 0;
            
            if (shouldPrint) {
                String msg = "[ExecutorServiceTask #" + count + "] Processing " + keys.size() 
                        + " keys on " + threadName;
                System.out.println(msg);
                System.err.println(msg);
                System.out.flush();
                System.err.flush();
            }
            
            // Get HazelcastInstance from member
            HazelcastInstance hz = Hazelcast.getAllHazelcastInstances().iterator().next();
            IMap<String, Long> map = hz.getMap(mapName);
            
            // Simulate work - this runs on executor thread, NOT partition thread!
            Thread.sleep(workDurationMillis);
            
            // Access map and perform operations
            // This won't block partition threads!
            for (String key : keys) {
                Long value = map.get(key);
                map.put(key, value != null ? value + 1 : 1L);
            }
            
            return null;
        }
    }
}
