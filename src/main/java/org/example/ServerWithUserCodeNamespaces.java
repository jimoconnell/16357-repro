    package org.example;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.UserCodeNamespaceConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import java.io.File;
import java.net.URL;

/**
 * Example server configuration with User Code Namespaces enabled.
 * This shows how to configure the server to accept EntryProcessor classes from clients.
 * 
 * To use this:
 * 1. Update the jarPath to point to your JAR file containing the EntryProcessor classes
 * 2. Make sure the cluster name matches your client configuration ("dev")
 * 3. Run this server before starting your client
 */
public class ServerWithUserCodeNamespaces {
    
    private static final String TRADES_MAP = "trades";
    private static final String POSITIONS_MAP = "positions";
    private static final String NAMESPACE_NAME = "ep-namespace";
    
    public static void main(String[] args) {
        Config cfg = new Config();
        cfg.setClusterName("dev");  // Match your client cluster name
        
        String license = System.getenv("HZ_LICENSEKEY");
        if (license == null || license.isEmpty()) {
            System.err.println("WARNING: HZ_LICENSEKEY not set, cluster may start in OSS mode.");
        } else {
            cfg.setLicenseKey(license);
        }
        
        // Configure User Code Namespace
        // Update this path to point to your JAR file
        String jarPath = System.getProperty("user.dir") + "/target/hazelcast-noise-client-1.0-SNAPSHOT.jar";
        File jarFile = new File(jarPath);
        
        if (!jarFile.exists()) {
            System.err.println("WARNING: JAR file not found at: " + jarPath);
            System.err.println("Please update jarPath or ensure the JAR is built (mvn package)");
        } else {
            try {
                // Enable User Code Namespaces
                cfg.getNamespacesConfig().setEnabled(true);
                
                // Create namespace configuration
                UserCodeNamespaceConfig namespaceConfig = new UserCodeNamespaceConfig(NAMESPACE_NAME);
                // Add JAR with file:// URL
                URL jarUrl = jarFile.toURI().toURL();
                namespaceConfig.addJar(jarUrl, "ep-jar");
                cfg.getNamespacesConfig().addNamespaceConfig(namespaceConfig);
                System.out.println("Configured User Code Namespace '" + NAMESPACE_NAME + "' with JAR: " + jarPath);
            } catch (Exception e) {
                System.err.println("ERROR: Failed to configure User Code Namespace: " + e.getMessage());
                e.printStackTrace();
            }
        }
        
        // Configure maps and associate them with the namespace
        MapConfig tradesMapConfig = new MapConfig(TRADES_MAP);
        tradesMapConfig.setUserCodeNamespace(NAMESPACE_NAME);
        cfg.addMapConfig(tradesMapConfig);
        
        MapConfig positionsMapConfig = new MapConfig(POSITIONS_MAP);
        positionsMapConfig.setUserCodeNamespace(NAMESPACE_NAME);
        cfg.addMapConfig(positionsMapConfig);
        
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(cfg);
        
        System.out.println("Server started with User Code Namespaces enabled");
        System.out.println("Cluster name: " + cfg.getClusterName());
        System.out.println("Namespace: " + NAMESPACE_NAME);
        System.out.println("Maps configured: " + TRADES_MAP + ", " + POSITIONS_MAP);
        
        // Keep server running
        Runtime.getRuntime().addShutdownHook(new Thread(hz::shutdown));
    }
}

