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

public class OffloadableTwoMapLockReproducer {

    private static final String TRADES_MAP = "trades";
    private static final String POSITIONS_MAP = "positions";

    public static void main(String[] args) throws Exception {

        long epSleepMillis = 2000;
        int hotKeyCount = 1000;
        int getAllBatchSize = 1000;

        Config cfg = new Config();
        cfg.setClusterName("offloadable-two-map-lock-reproducer");

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

        // Same keys in both maps so they share partitions
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
        System.out.println("Mode: Positions lock + Offloadable EP, Trades getAll");
        System.out.println("EP sleep millis: " + epSleepMillis);
        System.out.println("Hot keys: " + hotKeys.size());
        System.out.println("getAll batch size: " + getAllKeys.size());
        System.out.println("tradesMap name: " + TRADES_MAP);
        System.out.println("positionsMap name: " + POSITIONS_MAP);

        Thread positionsThread = new Thread(
                () -> runPositionsLockOffloadableLoop(positionsMap, hotKeys, epSleepMillis),
                "positions-offloadable-ep-thread"
        );
        positionsThread.setDaemon(true);
        positionsThread.start();

        runTradesGetAllDriver(tradesMap, getAllKeys);

        hz.shutdown();
    }

    // Positions: lock -> Offloadable EP -> unlock, on hot keys
    private static void runPositionsLockOffloadableLoop(IMap<String, Long> positionsMap,
                                                        List<String> keys,
                                                        long sleepMillis) {
        while (true) {
            long txStart = System.nanoTime();
            try {
                // lock all hot keys
                for (String key : keys) {
                    positionsMap.lock(key);
                }

                // execute Offloadable EP on each hot key
                for (String key : keys) {
                    EntryProcessor<String, Long, Void> ep =
                            new OffloadableSleepEP(sleepMillis);
                    positionsMap.executeOnKey(key, ep);
                }

            } finally {
                // unlock all hot keys
                for (String key : keys) {
                    try {
                        positionsMap.unlock(key);
                    } catch (Exception ignore) {
                    }
                }
            }

            long txMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - txStart);
            System.out.println(
                    "[positions] TX finished in " + txMs +
                            " ms, sleep=" + sleepMillis + " ms"
            );
        }
    }

    // Trades: loop getAll on the same keys, measure latency
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

    // Offloadable EP that simulates heavy work and logs the thread name
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
            String threadName = Thread.currentThread().getName();
            System.out.println("[OffloadableEP] " + entry.getKey() + " on " + threadName);
            try {
                Thread.sleep(sleepMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return null;
        }
    }
}