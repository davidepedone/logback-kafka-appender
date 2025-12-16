package ch.davidepedone.logback.kafka.util;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.util.HashMap;
import java.util.Map;

public class TestKafka {

	private final EmbeddedKafkaCluster kafkaCluster;

	public KafkaConsumer<byte[], byte[]> createClient() {
		return createClient(new HashMap<>());
	}

	public KafkaConsumer<byte[], byte[]> createClient(Map<String, Object> consumerProperties) {
		consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getBrokerList());
		consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		consumerProperties.put("auto.offset.reset", "earliest");
		consumerProperties.put("key.deserializer", ByteArrayDeserializer.class.getName());
		consumerProperties.put("value.deserializer", ByteArrayDeserializer.class.getName());
		return new KafkaConsumer<>(consumerProperties);
	}

	TestKafka(EmbeddedKafkaCluster kafkaCluster) {
		this.kafkaCluster = kafkaCluster;
	}

	public static TestKafka createTestKafka(int brokerCount, int partitionCount, int replicationFactor)
			throws Exception {
		final EmbeddedKafkaCluster kafka = new EmbeddedKafkaCluster(brokerCount, partitionCount, replicationFactor);
		kafka.startup();
		return new TestKafka(kafka);
	}

	public String getBrokerList() {
		return kafkaCluster.getBrokerList();
	}

	public void shutdown() {
		try {
			kafkaCluster.shutdown();
		}
		catch (Exception e) {
			System.out.println("Exception shutting down test cluster");
			e.printStackTrace();
		}
	}

}
