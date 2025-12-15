package ch.davidepedone.logback.kafka.util;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestKafka {

    private final EmbeddedKafkaCluster kafkaCluster;

    public KafkaConsumer<byte[], byte[]> createClient() {
        return createClient(new HashMap<String, Object>());
    }

    public KafkaConsumer<byte[], byte[]> createClient(Map<String, Object> consumerProperties) {
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getBrokerList());
        consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProperties.put("auto.offset.reset","earliest");
        consumerProperties.put("key.deserializer", ByteArrayDeserializer.class.getName());
        consumerProperties.put("value.deserializer", ByteArrayDeserializer.class.getName());
        return new KafkaConsumer<>(consumerProperties);
    }


    TestKafka(EmbeddedKafkaCluster kafkaCluster) {
        this.kafkaCluster = kafkaCluster;
    }

    public static TestKafka createTestKafka(int brokerCount, int partitionCount, int replicationFactor) throws Exception {
        final List<Integer> ports = new ArrayList<Integer>(brokerCount);
        for (int i=0; i<brokerCount; ++i) {
            ports.add(-1);
        }
        final Map<String, String> properties = new HashMap<>();
        properties.put("num.partitions", Integer.toString(partitionCount));
        properties.put("default.replication.factor", Integer.toString(replicationFactor));

        return createTestKafka(properties);
    }

    public static TestKafka createTestKafka() throws Exception {
        return createTestKafka(Collections.<String, String>emptyMap());
    }

    public static TestKafka createTestKafka(Map<String, String> properties)
        throws Exception {
        if (properties == null) properties = Collections.emptyMap();


        final EmbeddedKafkaCluster kafka = new EmbeddedKafkaCluster(properties);
        kafka.startup();
        return new TestKafka(kafka);
    }

    public String getBrokerList() {
        return kafkaCluster.getBrokerList();
    }

    public void shutdown() {
        try {
            kafkaCluster.shutdown();
        } finally {
//            zookeeper.shutdown();
        }
    }

    public void awaitShutdown() {
        // TODO cleanup
        // removed since kafkaCluster.close() is blocking
//        kafkaCluster.awaitShutdown();
    }


}
