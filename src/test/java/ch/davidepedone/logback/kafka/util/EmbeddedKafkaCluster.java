package ch.davidepedone.logback.kafka.util;

import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;

import java.util.concurrent.ExecutionException;

public class EmbeddedKafkaCluster {

	private final KafkaClusterTestKit cluster;

	public EmbeddedKafkaCluster(int brokerCount, int partitionCount, int replicationFactor) throws Exception {
		// Single-node KRaft cluster (controller + broker)
		TestKitNodes nodes = new TestKitNodes.Builder().setNumBrokerNodes(brokerCount).setNumControllerNodes(1).build();

		cluster = new KafkaClusterTestKit.Builder(nodes).setConfigProp("auto.create.topics.enable", "true")
			.setConfigProp("num.partitions", Integer.toString(partitionCount))
			.setConfigProp("default.replication.factor", Integer.toString(replicationFactor))
			.setConfigProp("offsets.topic.replication.factor", Integer.toString(replicationFactor))
			.setConfigProp("transaction.state.log.replication.factor", Integer.toString(replicationFactor))
			.setConfigProp("transaction.state.log.min.isr", "1")
			.build();

		cluster.format(); // formats KRaft metadata
	}

	public void startup() throws ExecutionException, InterruptedException {
		cluster.startup(); // starts broker + controller
		cluster.waitForReadyBrokers();
	}

	public String getBrokerList() {
		return cluster.bootstrapServers();
	}

	public void shutdown() throws Exception {
		cluster.close();
	}

	@Override
	public String toString() {
		return "EmbeddedKafkaCluster{" + "boostrapServers='" + getBrokerList() + '\'' + '}';
	}

}
