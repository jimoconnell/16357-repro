package org.example;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;

import java.io.Serializable;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

/**
 * Example showing how to use ExecutorService instead of EntryProcessors
 * when you need the lock-EP-unlock pattern.
 * 
 * This approach:
 * 1. Locks keys on the client side
 * 2. Submits a task to ExecutorService to run on a member
 * 3. The task accesses the map and performs work
 * 4. Unlocks keys after completion
 * 
 * This avoids blocking partition threads because the work runs on
 * executor threads, not partition threads.
 */
public class ExecutorServiceExample {
    
    private static final String POSITIONS_MAP = "positions";
    
    public static void main(String[] args) throws Exception {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClusterName("dev");
        clientConfig.getNetworkConfig().addAddress("localhost:5701");
        
        HazelcastInstance hz = HazelcastClient.newHazelcastClient(clientConfig);
        IMap<String, Long> positionsMap = hz.getMap(POSITIONS_MAP);
        IExecutorService executorService = hz.getExecutorService("default");
        
        // Example: Lock a set of keys, process them, then unlock
        Set<String> keysToProcess = Set.of("K1", "K2", "K3");
        
        // Step 1: Lock all keys
        for (String key : keysToProcess) {
            positionsMap.lock(key);
        }
        
        try {
            // Step 2: Submit work to ExecutorService (runs on member, NOT partition thread)
            Future<Void> future = executorService.submit(
                new ProcessPositionsTask(keysToProcess, 2000L)
            );
            
            // Step 3: Wait for completion (or do other work)
            future.get(); // Blocks until task completes
            
        } finally {
            // Step 4: Always unlock
            for (String key : keysToProcess) {
                positionsMap.unlock(key);
            }
        }
        
        hz.shutdown();
    }
    
    /**
     * Task that runs on ExecutorService thread (NOT partition thread).
     * This task can access maps, perform long-running work, etc.
     */
    public static class ProcessPositionsTask 
            implements Callable<Void>, Serializable {
        
        private final Set<String> keys;
        private final long workDurationMillis;
        
        public ProcessPositionsTask(Set<String> keys, long workDurationMillis) {
            this.keys = keys;
            this.workDurationMillis = workDurationMillis;
        }
        
        @Override
        public Void call() throws Exception {
            // This runs on an executor thread, NOT a partition thread!
            System.out.println("Task running on: " + Thread.currentThread().getName());
            System.out.println("Processing keys: " + keys);
            
            // Get HazelcastInstance - Hazelcast provides it via HazelcastContext
            // or you can get it from Hazelcast.getAllHazelcastInstances()
            HazelcastInstance hz = Hazelcast.getAllHazelcastInstances().iterator().next();
            IMap<String, Long> map = hz.getMap(POSITIONS_MAP);
            
            // Simulate work - this won't block partition threads!
            Thread.sleep(workDurationMillis);
            
            // Access maps, perform operations, etc.
            // Since we're not on a partition thread, we can do long-running work
            // without blocking other operations
            for (String key : keys) {
                Long value = map.get(key);
                // Perform your work here
                map.put(key, value != null ? value + 1 : 1L);
            }
            
            return null;
        }
    }
    
    /**
     * Alternative: Task that receives HazelcastInstance as a parameter.
     * This is more practical for real-world usage.
     */
    public static class ProcessPositionsTaskWithHz 
            implements Callable<Void>, Serializable {
        
        private final Set<String> keys;
        private final long workDurationMillis;
        private final String mapName;
        
        // Note: HazelcastInstance is not Serializable, so we pass map name
        // and get instance inside the task
        public ProcessPositionsTaskWithHz(Set<String> keys, 
                                         long workDurationMillis,
                                         String mapName) {
            this.keys = keys;
            this.workDurationMillis = workDurationMillis;
            this.mapName = mapName;
        }
        
        @Override
        public Void call() throws Exception {
            // Get HazelcastInstance from Hazelcast
            HazelcastInstance hz = Hazelcast.getAllHazelcastInstances().iterator().next();
            IMap<String, Long> map = hz.getMap(mapName);
            
            System.out.println("Task running on: " + Thread.currentThread().getName());
            System.out.println("Processing keys: " + keys);
            
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

