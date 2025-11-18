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

public class LockEpReproducer {

    private static final String POSITIONS_MAP = "positions";

    public static void main(String[] args) throws Exception {

        // Flags
        boolean useOffloadable = Arrays.asList(args).contains("--offloadable");
        boolean locksOnly = Arrays.asList(args).contains("--locks-only");
        boolean noLocks = Arrays.asList(args).contains("--no-locks");

        // Configurable parameters
        long epSleepMillis = 2000;       // simulate heavy EP
        int lockedKeyCount = 1000;       // number of keys to lock
        int getAllBatchSize = 1000;      // number of keys per getAll

        // Hazelcast config
        Config cfg = new Config();
        cfg.setClusterName("dev");

        // license from environment variable
        String license = System.getenv("HZ_LICENSEKEY");
        if (license == null || license.isEmpty()) {
            System.err.println("WARNING: HZ_LICENSEKEY not set, cluster may start in OSS mode.");
        } else {
            cfg.setLicenseKey(license);
        }

        cfg.addMapConfig(new MapConfig(POSITIONS_MAP));

        HazelcastInstance hz = Hazelcast.newHazelcastInstance(cfg);
        IMap<String, Long> positions = hz.getMap(POSITIONS_MAP);

        // Prepopulate keys
        List<String> allKeys = new ArrayList<>();
        for (int i = 0; i < 50_000; i++) {
            String key = "P" + i;
            allKeys.add(key);
            positions.put(key, 0L);
        }

        if (lockedKeyCount > allKeys.size()) lockedKeyCount = allKeys.size();
        if (getAllBatchSize > allKeys.size()) getAllBatchSize = allKeys.size();

        List<String> lockedKeys = allKeys.subList(0, lockedKeyCount);
        List<String> getAllKeys = allKeys.subList(0, getAllBatchSize);

        // Print setup
        System.out.println("Started member, keys=" + allKeys.size());
        System.out.println("Using offloadable EP: " + useOffloadable);
        System.out.println("Locks only (no EP): " + locksOnly);
        System.out.println("No locks: " + noLocks);
        System.out.println("EP sleep millis: " + epSleepMillis);
        System.out.println("Locked keys: " + lockedKeys.size());
        System.out.println("getAll batch size: " + getAllKeys.size());

        // Background thread running lock -> EP -> unlock (or lock -> sleep -> unlock)
        Thread epThread = new Thread(
                () -> runLockEpLoop(
                        positions,
                        lockedKeys,
                        epSleepMillis,
                        useOffloadable,
                        locksOnly,
                        noLocks
                ),
                "ep-thread"
        );

        epThread.setDaemon(true);
        epThread.start();

        // Foreground: repeatedly call getAll and log latency
        runGetAllDriver(positions, getAllKeys);

        hz.shutdown();
    }

    private static void runLockEpLoop(IMap<String, Long> map,
                                      List<String> keys,
                                      long sleepMillis,
                                      boolean useOffloadable,
                                      boolean locksOnly,
                                      boolean noLocks) {
        while (true) {
            long txStart = System.nanoTime();
            try {

                // Optionally lock
                if (!noLocks) {
                    for (String key : keys) {
                        map.lock(key);
                    }
                }

                if (locksOnly) {
                    // hold locks for the duration
                    try {
                        Thread.sleep(sleepMillis);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }

                } else {
                    // Execute the EP on each key
                    for (String key : keys) {
                        EntryProcessor<String, Long, Void> ep =
                                useOffloadable
                                        ? new OffloadableSleepEP(sleepMillis)
                                        : new SleepEP(sleepMillis);
                        map.executeOnKey(key, ep);
                    }
                }

            } finally {
                // unlock only if we locked
                if (!noLocks) {
                    for (String key : keys) {
                        try {
                            map.unlock(key);
                        } catch (Exception ignore) {
                        }
                    }
                }
            }

            long txMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - txStart);
            System.out.println(
                    "TX finished in " + txMs +
                            " ms, sleep=" + sleepMillis +
                            " ms, locksOnly=" + locksOnly +
                            ", offloadable=" + useOffloadable +
                            ", noLocks=" + noLocks
            );
        }
    }

    private static void runGetAllDriver(IMap<String, Long> map,
                                        List<String> keys) throws InterruptedException {
        Random rnd = new Random();

        while (true) {
            List<String> batch = new ArrayList<>(keys);
            Collections.shuffle(batch, rnd);

            long start = System.nanoTime();
            Map<String, Long> result = map.getAll(new HashSet<>(batch));
            long durationMicros = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - start);

            System.out.printf(Locale.ROOT,
                    "getAll latency %d µs, size=%d%n",
                    durationMicros,
                    result.size()
            );

            Thread.sleep(100);
        }
    }

    // EP running on the partition thread
    public static class SleepEP implements EntryProcessor<String, Long, Void>, Serializable {
        private final long sleepMillis;

        public SleepEP(long sleepMillis) {
            this.sleepMillis = sleepMillis;
        }

        @Override
        public Void process(Map.Entry<String, Long> entry) {
            try {
                Thread.sleep(sleepMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return null;
        }
    }

    // EP running on offloadable executor for heavy work
    public static class OffloadableSleepEP implements EntryProcessor<String, Long, Void>,
            Offloadable, Serializable {

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
                Thread.sleep(sleepMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return null;
        }
    }
}