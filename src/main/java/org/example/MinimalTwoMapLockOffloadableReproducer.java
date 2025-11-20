package org.example;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Offloadable;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.IMap;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class MinimalTwoMapLockOffloadableReproducer {

    private static final String TRADES_MAP = "trades";
    private static final String POSITIONS_MAP = "positions";

    // Hardcoded parameters
    private static final int KEY_COUNT = 10_000;
    private static final int HOT_KEY_COUNT = 500;
    private static final int GETALL_BATCH_SIZE = 500;
    private static final long EP_SLEEP_MILLIS = 2000L;

    public static void main(String[] args) throws Exception {
        Config cfg = new Config();
        cfg.setClusterName("minimal-two-map-lock-offloadable");
        String license = System.getenv("HZ_LICENSEKEY");
        if (license == null || license.isEmpty()) {
            System.err.println("WARNING: HZ_LICENSEKEY not set, cluster may start in OSS mode.");
        } else {
            cfg.setLicenseKey(license);
        }
        // Use default in memory format etc
        cfg.addMapConfig(new MapConfig(TRADES_MAP));
        cfg.addMapConfig(new MapConfig(POSITIONS_MAP));

        HazelcastInstance hz = Hazelcast.newHazelcastInstance(cfg);

        IMap<String, Long> trades = hz.getMap(TRADES_MAP);
        IMap<String, Long> positions = hz.getMap(POSITIONS_MAP);

        // Create identical keys in both maps so they share partitions
        for (int i = 0; i < KEY_COUNT; i++) {
            String key = "K" + i;
            trades.put(key, 0L);
            positions.put(key, 0L);
        }

        System.out.println("Started member, keys=" + KEY_COUNT);
        System.out.println("Mode: Positions lock + Offloadable EP, Trades getAll");
        System.out.println("EP sleep millis: " + EP_SLEEP_MILLIS);
        System.out.println("Hot keys: " + HOT_KEY_COUNT);
        System.out.println("getAll batch size: " + GETALL_BATCH_SIZE);
        System.out.println("tradesMap name: " + TRADES_MAP);
        System.out.println("positionsMap name: " + POSITIONS_MAP);

        // Background thread: lock + Offloadable EP on positions
        Thread positionsThread = new Thread(() ->
                runPositionsLockEpLoop(positions), "positions-lock-ep-thread");
        positionsThread.setDaemon(true);
        positionsThread.start();

        // Main thread: Trades getAll loop
        runTradesGetAllLoop(trades);

        hz.shutdown();
    }

    private static void runPositionsLockEpLoop(IMap<String, Long> positions) {
        try {
            while (true) {
                long txStart = System.nanoTime();

                // Lock a fixed range of keys K0..K(HOT_KEY_COUNT-1)
                try {
                    for (int i = 0; i < HOT_KEY_COUNT; i++) {
                        String key = "K" + i;
//                        positions.lock(key);
                    }

                    // Run Offloadable EP on the same keys
                    for (int i = 0; i < HOT_KEY_COUNT; i++) {
                        String key = "K" + i;
                        EntryProcessor<String, Long, Void> ep =
                                new SlowOffloadableEp(EP_SLEEP_MILLIS);
                        positions.executeOnKey(key, ep);
                    }
                } finally {
                    // Always unlock
                    for (int i = 0; i < HOT_KEY_COUNT; i++) {
                        String key = "K" + i;
                        try {
                            positions.unlock(key);
                        } catch (Exception ignore) {
                        }
                    }
                }

                long txMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - txStart);
                System.out.println("[positions] TX finished in " + txMs + " ms");
            }
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    private static void runTradesGetAllLoop(IMap<String, Long> trades) throws InterruptedException {
        // Fixed batch K0..K(GETALL_BATCH_SIZE-1)
        HashSet<String> batch = new HashSet<>();
        for (int i = 0; i < GETALL_BATCH_SIZE; i++) {
            batch.add("K" + i);
        }

        while (true) {
            long start = System.nanoTime();
            Map<String, Long> result = trades.getAll(batch);
            long durationMicros = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - start);

            System.out.printf(Locale.ROOT,
                    "[trades] getAll latency %d µs, size=%d%n",
                    durationMicros,
                    result.size()
            );

            Thread.sleep(100);
        }
    }

    public static class SlowOffloadableEp
            implements EntryProcessor<String, Long, Void>, Offloadable, Serializable {

        private final long sleepMillis;

        public SlowOffloadableEp(long sleepMillis) {
            this.sleepMillis = sleepMillis;
        }

        @Override
        public String getExecutorName() {
            return Offloadable.OFFLOADABLE_EXECUTOR;
        }

        @Override
        public Void process(Map.Entry<String, Long> entry) {
            String threadName = Thread.currentThread().getName();
            System.out.println("[OffloadableEP] " + entry.getKey()
                    + " on " + threadName);
            try {
                Thread.sleep(sleepMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return null;
        }
    }
}