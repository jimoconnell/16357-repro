package org.example;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.UserCodeNamespaceConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import java.io.File;
import java.net.URL;

/**
 * Standalone Hazelcast member with User Code Namespaces configured.
 * 
 * This starts a Hazelcast member programmatically with:
 * - Cluster name: "dev" (matches client configuration)
 * - User Code Namespaces enabled
 * - EntryProcessor classes deployed via UCN
 * - Maps configured to use the namespace
 * 
 * Usage:
 *   mvn exec:java -Dexec.mainClass="org.example.StandaloneMemberWithUCN"
 * 
 * Or run from IDE after building the JAR:
 *   mvn package
 *   java -cp target/hazelcast-noise-client-1.0-SNAPSHOT.jar org.example.StandaloneMemberWithUCN
 */
public class StandaloneMemberWithUCN {
    
    private static final String TRADES_MAP = "trades";
    private static final String POSITIONS_MAP = "positions";
    private static final String NAMESPACE_NAME = "ep-namespace";
    
    public static void main(String[] args) {
        System.out.println("=== Starting Hazelcast Member with User Code Namespaces ===");
        
        Config cfg = new Config();
        cfg.setClusterName("dev");  // Match client cluster name
        
        // Configure license from environment variable
        String license = System.getenv("HZ_LICENSEKEY");
        if (license == null || license.isEmpty()) {
            System.err.println("WARNING: HZ_LICENSEKEY not set, cluster may start in OSS mode.");
        } else {
            cfg.setLicenseKey(license);
            System.out.println("License key configured from HZ_LICENSEKEY environment variable");
        }
        
        // Configure network - bind to all interfaces for portability
        cfg.getNetworkConfig().getInterfaces().setEnabled(false); // Allow all interfaces
        cfg.getNetworkConfig().setPort(5701);
        
        // Enable metrics
        cfg.getMetricsConfig().setEnabled(true);
        cfg.getMetricsConfig().getManagementCenterConfig().setEnabled(true);
        cfg.getMetricsConfig().getJmxConfig().setEnabled(true);
        cfg.getMetricsConfig().setCollectionFrequencySeconds(1);
        
        // Enable diagnostics and configure output directory
        String diagnosticsDir = System.getProperty("user.dir") + "/diagnostics";
        cfg.setProperty("hazelcast.diagnostics.enabled", "true");
        cfg.setProperty("hazelcast.diagnostics.directory", diagnosticsDir);
        cfg.setProperty("hazelcast.diagnostics.metric.period.seconds", "1");
        System.out.println("✓ Diagnostics enabled - output directory: " + diagnosticsDir);
        
        // Configure User Code Namespace
        String jarPath = findJarPath();
        File jarFile = new File(jarPath);
        
        if (!jarFile.exists()) {
            System.err.println("ERROR: JAR file not found at: " + jarPath);
            System.err.println("Please run 'mvn package' first to build the JAR");
            System.exit(1);
        }
        
        try {
            // Enable User Code Namespaces
            cfg.getNamespacesConfig().setEnabled(true);
            
            // Create namespace configuration
            UserCodeNamespaceConfig namespaceConfig = new UserCodeNamespaceConfig(NAMESPACE_NAME);
            URL jarUrl = jarFile.toURI().toURL();
            namespaceConfig.addJar(jarUrl, "ep-jar");
            cfg.getNamespacesConfig().addNamespaceConfig(namespaceConfig);
            
            System.out.println("✓ User Code Namespace '" + NAMESPACE_NAME + "' configured");
            System.out.println("  JAR: " + jarPath);
        } catch (Exception e) {
            System.err.println("ERROR: Failed to configure User Code Namespace: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
        
        // Configure maps and associate them with the namespace
        MapConfig tradesMapConfig = new MapConfig(TRADES_MAP);
        tradesMapConfig.setUserCodeNamespace(NAMESPACE_NAME);
        cfg.addMapConfig(tradesMapConfig);
        
        MapConfig positionsMapConfig = new MapConfig(POSITIONS_MAP);
        positionsMapConfig.setUserCodeNamespace(NAMESPACE_NAME);
        cfg.addMapConfig(positionsMapConfig);
        
        System.out.println("✓ Maps configured: " + TRADES_MAP + ", " + POSITIONS_MAP);
        System.out.println("  Both maps associated with namespace: " + NAMESPACE_NAME);
        
        // Start the member
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(cfg);
        
        System.out.println();
        System.out.println("=== Member Started Successfully ===");
        System.out.println("Cluster name: " + cfg.getClusterName());
        System.out.println("Member address: " + hz.getCluster().getLocalMember().getAddress());
        System.out.println("Member UUID: " + hz.getCluster().getLocalMember().getUuid());
        System.out.println("Diagnostics directory: " + diagnosticsDir);
        System.out.println("  (Diagnostic files will be written here during runtime)");
        System.out.println();
        System.out.println("Ready to accept client connections!");
        System.out.println("Press Ctrl+C to shutdown...");
        System.out.println();
        
        // Keep server running until shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\nShutting down Hazelcast member...");
            hz.shutdown();
            System.out.println("Shutdown complete.");
        }));
        
        // Keep main thread alive
        try {
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
    
    /**
     * Find the JAR file path. Tries multiple locations for portability.
     */
    private static String findJarPath() {
        // Try 1: Relative to current working directory (most common)
        String relativePath = "target/hazelcast-noise-client-1.0-SNAPSHOT.jar";
        File relativeFile = new File(relativePath);
        if (relativeFile.exists()) {
            return relativeFile.getAbsolutePath();
        }
        
        // Try 2: Absolute path from user.dir
        String userDirPath = System.getProperty("user.dir") + "/target/hazelcast-noise-client-1.0-SNAPSHOT.jar";
        File userDirFile = new File(userDirPath);
        if (userDirFile.exists()) {
            return userDirFile.getAbsolutePath();
        }
        
        // Try 3: Check if running from JAR itself (get location of this class)
        try {
            String classPath = StandaloneMemberWithUCN.class.getProtectionDomain()
                    .getCodeSource().getLocation().getPath();
            if (classPath.endsWith(".jar")) {
                return classPath;
            }
        } catch (Exception e) {
            // Ignore
        }
        
        // Default fallback
        return relativePath;
    }
}

