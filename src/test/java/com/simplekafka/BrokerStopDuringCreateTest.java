package com.simplekafka;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

import org.apache.zookeeper.ZooKeeper;
import org.junit.jupiter.api.Test;

import com.simplekafka.broker.Protocol;
import com.simplekafka.client.SimpleKafkaClient;

class BrokerStopDuringCreateTest {

    // Broker 1 is the controller, so the create goes to it directly. It picks leaders from the brokers alive
    // when the create starts, so broker 3 leads about a third of the partitions written after it stopped.
    @Test
    void noPartitionIsLeftLedByABrokerThatStoppedDuringCreate() throws Exception {
        try (TestCluster cluster = TestCluster.start(3);
                ZooKeeper zk = cluster.openZooKeeper()) {
            CompletableFuture<ByteBuffer> created = CompletableFuture.supplyAsync(() -> {
                try {
                    return cluster.request(1, Protocol.encodeCreateTopicRequest("wide", 400, (short) 2));
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
            TestCluster.await("the controller to start writing partitions", () -> {
                try {
                    return zk.exists("/topics/wide/partitions/20", false) != null;
                } catch (Exception e) {
                    return false;
                }
            });
            cluster.stopBroker(3);
            ByteBuffer response = created.get();
            assertEquals(Protocol.CREATE_TOPIC_RESPONSE, response.get(), "response type");
            assertEquals(0, response.get(), "create status");

            TestCluster.await("every partition of wide led by a live broker in the controller's metadata", () -> {
                try {
                    return cluster.client(1).getTopicMetadata("wide").getPartitions().stream()
                            .allMatch(p -> p.getLeader() == 1 || p.getLeader() == 2);
                } catch (IOException e) {
                    return false;
                }
            });
            TestCluster.await("a produce to every partition of wide to succeed", () -> {
                try {
                    SimpleKafkaClient client = cluster.client(1);
                    for (int partition = 0; partition < 400; partition++) {
                        client.send("wide", partition, "m".getBytes(StandardCharsets.UTF_8));
                    }
                    return true;
                } catch (IOException e) {
                    return false;
                }
            });
        }
    }
}
