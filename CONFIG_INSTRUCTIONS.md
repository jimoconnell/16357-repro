# Adding User Code Namespaces to Your Hazelcast Server Config

## Step 1: Build the JAR

First, make sure the JAR file is built:

```bash
mvn package
```

This creates: `target/hazelcast-noise-client-1.0-SNAPSHOT.jar`

## Step 2: Update Your hazelcast.xml

Add the following sections to your `hazelcast.xml` configuration file:

### 1. Add User Code Namespaces Configuration

Add this section **right after** the `<cluster-name>dev</cluster-name>` line (around line 50):

```xml
<!-- Enable User Code Namespaces -->
<user-code-namespaces enabled="true">
    <namespace name="ep-namespace">
        <jar id="ep-jar">
            <!-- Update this path to match where your JAR file is located -->
            <url>file:///Users/jim/projects/16357-repro/target/hazelcast-noise-client-1.0-SNAPSHOT.jar</url>
        </jar>
    </namespace>
</user-code-namespaces>
```

**Important:** Update the `<url>` path to point to the actual location of your JAR file. You can use:
- Absolute path: `file:///absolute/path/to/jar`
- Relative path from where Hazelcast runs

### 2. Add Map Configurations

Add these map configurations **after** the existing `<map name="default">` section ends (around line 400):

```xml
<!-- Map configuration for trades with User Code Namespace -->
<map name="trades">
    <in-memory-format>BINARY</in-memory-format>
    <backup-count>1</backup-count>
    <async-backup-count>0</async-backup-count>
    <time-to-live-seconds>0</time-to-live-seconds>
    <max-idle-seconds>0</max-idle-seconds>
    <eviction eviction-policy="NONE" max-size-policy="PER_NODE" size="0"/>
    <merge-policy batch-size="100">com.hazelcast.spi.merge.PutIfAbsentMergePolicy</merge-policy>
    <cache-deserialized-values>INDEX-ONLY</cache-deserialized-values>
    <statistics-enabled>true</statistics-enabled>
    <per-entry-stats-enabled>false</per-entry-stats-enabled>
    <user-code-namespace>ep-namespace</user-code-namespace>
</map>

<!-- Map configuration for positions with User Code Namespace -->
<map name="positions">
    <in-memory-format>BINARY</in-memory-format>
    <backup-count>1</backup-count>
    <async-backup-count>0</async-backup-count>
    <time-to-live-seconds>0</time-to-live-seconds>
    <max-idle-seconds>0</max-idle-seconds>
    <eviction eviction-policy="NONE" max-size-policy="PER_NODE" size="0"/>
    <merge-policy batch-size="100">com.hazelcast.spi.merge.PutIfAbsentMergePolicy</merge-policy>
    <cache-deserialized-values>INDEX-ONLY</cache-deserialized-values>
    <statistics-enabled>true</statistics-enabled>
    <per-entry-stats-enabled>false</per-entry-stats-enabled>
    <user-code-namespace>ep-namespace</user-code-namespace>
</map>
```

## Step 3: Restart Your Hazelcast Server

After updating the configuration, restart your Hazelcast server for the changes to take effect.

## Step 4: Test the Client

Now your client should be able to connect and use EntryProcessors without ClassNotFoundException errors!

## Alternative: Use Absolute Path

If your JAR is in a different location, you can use an absolute path. For example, if you copy the JAR to a shared location:

```xml
<url>file:///opt/hazelcast/libs/hazelcast-noise-client-1.0-SNAPSHOT.jar</url>
```

Or if you want to use a path relative to where Hazelcast is running, you can use:

```xml
<url>file://./libs/hazelcast-noise-client-1.0-SNAPSHOT.jar</url>
```


