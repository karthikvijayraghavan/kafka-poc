package com.couchbase.kafka.kafka_poc.consumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.couchbase.kafka.kafka_poc.consumer.bo.CouchbaseEntity;
import com.couchbase.kafka.kafka_poc.consumer.mysql.MysqlDAO;
import com.couchbase.kafka.kafka_poc.consumer.util.JsonToObject;



public class SampleKafkaConsumer implements Runnable {
	private final KafkaConsumer<String, String> consumer;
	private final List<String> topics;
	private final int id;

	public SampleKafkaConsumer(int id, String groupId, List<String> topics, String kafkaServer) {
		this.id = id;
		this.topics = topics;
		Properties props = new Properties();
		props.put("bootstrap.servers", kafkaServer);
		props.put("group.id", groupId);
		props.put("key.deserializer", StringDeserializer.class.getName());
		props.put("value.deserializer", StringDeserializer.class.getName());
		this.consumer = new KafkaConsumer<String, String>(props);
	}

	public void run() {
		try {
			//To begin consumption, you must first subscribe to the topics your application needs to read from.
			consumer.subscribe(topics);

			while (true) {
				//The parameter passed to poll controls the maximum amount of time that the consumer will block while it awaits records at the current position.
				ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
				for (ConsumerRecord<String, String> record : records) {
					Map<String, Object> data = new HashMap<String, Object>();
					data.put("partition", record.partition());
					data.put("offset", record.offset());
					data.put("value", record.value());
					System.out.println(this.id + ": " + data);

					String couchbaseRecord = record.value();
					CouchbaseEntity couchbaseEntity = JsonToObject.convertJsonStringToObject(couchbaseRecord);
					MysqlDAO.populateData(couchbaseEntity);
					System.err.println("*************Update to RDBMS Completed************");

				}
			}
		} catch (WakeupException e) {
			// ignore for shutdown
		} finally {
			consumer.close();
		}
	}

	public void shutdown() {
		consumer.wakeup();
	}

	public static void main(String[] args) {
		int numConsumers = 3;
		String groupId = null;
		List<String> topics = null;
		String kafkaServer = null;
		if (args != null && args.length == 3) {
			kafkaServer = args[0];
			groupId = args[1];
			topics = Arrays.asList(args[2]);
		} else {
			System.out.println("Usage: java com.couchbase.example.kafka.consumer.CouchbaseConsumer ");
			System.out.println("$kafkaServer $groupId $topic");
			System.exit(0);
		}

		final ExecutorService executor = Executors.newFixedThreadPool(numConsumers);

		final List<SampleKafkaConsumer> consumers = new ArrayList<SampleKafkaConsumer>();
		for (int i = 0; i < numConsumers; i++) {
			SampleKafkaConsumer consumer = new SampleKafkaConsumer(i, groupId, topics, kafkaServer);
			consumers.add(consumer);
			executor.submit(consumer);
		}

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				for (SampleKafkaConsumer consumer : consumers) {
					consumer.shutdown();
				}
				executor.shutdown();
				try {
					executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		});
	}
}