package org.example;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import java.util.ArrayList;
import java.util.List;

public class StartHazelcastCluster {

	public static void main(String[] args) {
		// Specify the number of Hazelcast instances (nodes) to start
		int numberOfInstances = 3; // You can change this as needed
		int startingPort = 5701; // Starting port

		// Collect all ports in the cluster to configure TCP/IP discovery
		List<String> memberAddresses = new ArrayList<>();
		for (int i = 0; i < numberOfInstances; i++) {
			memberAddresses.add("127.0.0.1:" + (startingPort + i));
		}

		// Start multiple Hazelcast instances on different ports
		for (int i = 0; i < numberOfInstances; i++) {
			startHazelcastNode(startingPort + i, memberAddresses);
		}
	}

	private static void startHazelcastNode(int port, List<String> memberAddresses) {
		Config config = new Config();
		// Set the instance name for easier identification
		config.setInstanceName("HazelcastInstance-" + port);

		// Configure the network settings, including the port
		config.getNetworkConfig().setPort(port);
		config.getNetworkConfig().setPortAutoIncrement(false);

		// Configure TCP/IP for member discovery
		JoinConfig joinConfig = config.getNetworkConfig().getJoin();
		joinConfig.getMulticastConfig().setEnabled(false); // Disable multicast
		joinConfig.getTcpIpConfig().setEnabled(true); // Enable TCP/IP
		joinConfig.getTcpIpConfig().setMembers(memberAddresses); // Set the addresses of the members

		// Starting the Hazelcast instance (node)
		HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
		System.out.println("Started Hazelcast Node on port: " + port);
	}
}
