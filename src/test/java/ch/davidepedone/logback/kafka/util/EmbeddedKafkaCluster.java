package ch.davidepedone.logback.kafka.util;

import java.util.*;

import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.concurrent.ExecutionException;


public class EmbeddedKafkaCluster {
    private final Map<String, String> baseProperties;


    private KafkaClusterTestKit cluster;
    private final List<File> logDirs;


    public EmbeddedKafkaCluster(Map<String, String> baseProperties) throws Exception {

        this.baseProperties = baseProperties;

        this.logDirs = new ArrayList<>();


        // Single-node KRaft cluster (controller + broker)
        TestKitNodes nodes = new TestKitNodes.Builder()
            .setNumBrokerNodes(1)
            .setNumControllerNodes(1)
            .build();

        cluster = new KafkaClusterTestKit.Builder(nodes)
            .setConfigProp("auto.create.topics.enable", "true")
            .setConfigProp("offsets.topic.replication.factor", "1")
            .setConfigProp("transaction.state.log.replication.factor", "1")
            .setConfigProp("transaction.state.log.min.isr", "1")
            .build();

        cluster.format();   // formats KRaft metadata
    }

    public void startup() throws ExecutionException, InterruptedException {
        cluster.startup();  // starts broker + controller
        cluster.waitForReadyBrokers();
    }

    public String getBrokerList() {
        return cluster.bootstrapServers();
    }

    public void shutdown() {

        for (File logDir : logDirs) {
            try {
                TestUtils.deleteFile(logDir);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
    }

    public void awaitShutdown() throws Exception {
        cluster.close();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("EmbeddedKafkaCluster{");
        sb.append("boostrapServers='").append(getBrokerList()).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
