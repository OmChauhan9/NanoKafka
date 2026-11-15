package com.simplekafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.zookeeper.ZooKeeper;
import org.junit.jupiter.api.Test;

import com.simplekafka.broker.Protocol;
import com.simplekafka.client.SimpleKafkaClient;

class ProduceForwardingTest {

    // The controller's leader change reaches only the old leader, so each broker names the other as leader.
    @Test
    void aProduceBetweenTwoBrokersThatEachNameTheOtherLeaderIsAnswered() throws Exception {
        try (TestCluster cluster = TestCluster.start(2); ZooKeeper zk = cluster.openZooKeeper()) {
            SimpleKafkaClient admin = cluster.client(1);
            admin.createTopic("loop", 1, (short) 2);
            cluster.awaitTopicEverywhere("loop");
            int oldLeader = leaderOf(cluster.client(1));
            int newLeader = oldLeader == 1 ? 2 : 1;
            zk.setData("/topics/loop/partitions/0",
                    (newLeader + ";" + oldLeader + ",").getBytes(StandardCharsets.UTF_8), -1);
            assertEquals(0, cluster.request(oldLeader, Protocol.encodeTopicNotification("loop")).get());
            assertEquals(newLeader, leaderOf(cluster.client(oldLeader)), "leader named by broker " + oldLeader);
            assertEquals(oldLeader, leaderOf(cluster.client(newLeader)), "leader named by broker " + newLeader);

            int threadsBefore = Thread.activeCount();
            CompletableFuture<ByteBuffer> produce = CompletableFuture.supplyAsync(() -> {
                try {
                    return cluster.request(oldLeader,
                            Protocol.encodeProduceRequest("loop", 0, "x".getBytes(StandardCharsets.UTF_8)));
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
            ByteBuffer response = null;
            try {
                response = produce.get(3, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                fail("produce not answered within 3 s; JVM threads went from " + threadsBefore + " to "
                        + Thread.activeCount());
            }

            assertTrue(Thread.activeCount() - threadsBefore < 50,
                    "JVM threads went from " + threadsBefore + " to " + Thread.activeCount());
            Protocol.ProduceResult result = Protocol.decodeProduceResponse(response);
            assertFalse(result.isSuccess(), "produce acked at offset " + result.getOffset()
                    + " although neither broker leads the partition in its own view");
        }
    }

    private static int leaderOf(SimpleKafkaClient client) {
        return client.getTopicMetadata("loop").getPartitions().get(0).getLeader();
    }
}
