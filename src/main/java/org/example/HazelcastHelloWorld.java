package org.example;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

public class HazelcastHelloWorld {
	public static void main(String[] args) {
		// Configure the Hazelcast client
		HazelcastInstance client = HazelcastClient.newHazelcastClient();

		// Access the 'hello' map stored on the cluster
		IMap<String, String> map = client.getMap("hello");

		// Put the 'hello world' key-value pair into the map
		map.put("hello", "world");

		// Retrieve and print the value associated with the key 'hello'
		String value = map.get("hello");
		System.out.println("Key: 'hello', Value: '" + value + "'");

		// Shutdown the client to release resources
		client.shutdown();
	}
}
