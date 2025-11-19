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

public class OffloadableLockOnlyReproducer {

    private static final String POSITIONS_MAP = "positions";

    public static void main(String[] args) throws Exception {
        long epSleepMillis = 2000;
        int hotKeyCount = 1000;
        int getAllBatchSize = 1000;

        Config cfg = new Config();
        cfg.setClusterName("offloadable-lock-reproducer");

        // Optional license
        String license = System.getenv("HZ_LICENSEKEY");
        if (license == null || license.isEmpty()) {
            System.err.println("WARNING: HZ_LICENSEKEY not set, cluster may start in OSS mode.");
        } else {
            cfg.setLicenseKey(license);
        }

        cfg.addMapConfig(new MapConfig(POSITIONS_MAP));

        HazelcastInstance hz = Hazelcast.newHazelcastInstance(cfg);
        IMap<String, Long> positions = hz.getMap(POSITIONS_MAP);

        List<String> allKeys = new ArrayList<>();
        for (int i = 0; i < 50_000; i++) {
            String key = "K" + i;
            allKeys.add(key);
            positions.put(key, 0L);
        }

        if (hotKeyCount > allKeys.size()) hotKeyCount = allKeys.size();
        if (getAllBatchSize > allKeys.size()) getAllBatchSize = allKeys.size();

        List<String> hotKeys = allKeys.subList(0, hotKeyCount);
        List<String> getAllKeys = allKeys.subList(0, getAllBatchSize);

        System.out.println("Started member, keys=" + allKeys.size());
        System.out.println("Mode: Offloadable EP + explicit locks");
        System.out.println("EP sleep millis: " + epSleepMillis);
        System.out.println("Hot keys: " + hotKeys.size());
        System.out.println("getAll batch size: " + getAllKeys.size());
        System.out.println("positionsMap name: " + POSITIONS_MAP);

        Thread epThread = new Thread(
                () -> runLockOffloadableEpLoop(positions, hotKeys, epSleepMillis),
                "positions-offloadable-ep-thread"
        );
        epThread.setDaemon(true);
        epThread.start();

        runGetAllDriver(positions, getAllKeys);

        hz.shutdown();
    }

    private static void runLockOffloadableEpLoop(IMap<String, Long> positionsMap,
                                                 List<String> keys,
                                                 long sleepMillis) {
        while (true) {
            long txStart = System.nanoTime();
            try {
                // lock all hot keys
               for (String key : keys) {
                   positionsMap.lock(key);
               }

                // execute offloadable EP on each hot key
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
                    "[positions] getAll latency %d µs, size=%d%n",
                    durationMicros,
                    result.size()
            );

            Thread.sleep(100);
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