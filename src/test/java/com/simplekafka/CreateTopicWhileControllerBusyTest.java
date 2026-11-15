package com.simplekafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.ZooKeeper;
import org.junit.jupiter.api.Test;

import com.simplekafka.broker.Protocol;

/**
 * Each test first starts a create that waits 5 s on a silent broker, so the controller is busy when the
 * broker list changes and the next create arrives. The next create must still be judged by the brokers that are
 * live when it runs.
 */
class CreateTopicWhileControllerBusyTest {

    @Test
    void replicationFactorAboveTheLiveBrokersIsRefusedAfterABrokerLeaves() throws Exception {
        try (TestCluster cluster = TestCluster.start(1);
                ZooKeeper zk = cluster.openZooKeeper();
                ZooKeeper leavingSession = cluster.openZooKeeper();
                SilentBroker silent = new SilentBroker().register(zk, 8);
                SilentBroker leaving = new SilentBroker().register(leavingSession, 9)) {
            awaitBrokerCount(cluster, 1, 3);
            CompletableFuture<ByteBuffer> slow = createAsync(cluster, "slow", 1, (short) 1);
            awaitTopicInZooKeeper(zk, "slow");
            leavingSession.close();
            TestCluster.await("broker 9 gone from ZooKeeper", () -> {
                try {
                    return zk.exists("/brokers/9", false) == null;
                } catch (Exception e) {
                    return false;
                }
            });

            ByteBuffer response = cluster.request(1, Protocol.encodeCreateTopicRequest("rf3", 3, (short) 3));
            assertEquals(Protocol.ERROR_RESPONSE, response.get(), "response type");
            assertEquals("Invalid topic configuration", Protocol.readString(response), "error message");
            assertNull(zk.exists("/topics/rf3", false), "/topics/rf3 in ZooKeeper");
            assertCreated(slow);
        }
    }

    @Test
    void sizeLimitHoldsForABrokerIdThatJoinedWhileTheCreateWaited() throws Exception {
        try (TestCluster cluster = TestCluster.start(1);
                ZooKeeper zk = cluster.openZooKeeper();
                SilentBroker silent = new SilentBroker().register(zk, 9)) {
            awaitBrokerCount(cluster, 1, 2);
            CompletableFuture<ByteBuffer> slow = createAsync(cluster, "slow", 1, (short) 1);
            awaitTopicInZooKeeper(zk, "slow");
            try (SilentBroker wide = new SilentBroker().register(zk, 1_000_000_000L)) {
                // 13400 partitions fit in one ZooKeeper request when every broker id has one digit, and not when
                // a third of the leaders have ten.
                ByteBuffer response = cluster.request(1, Protocol.encodeCreateTopicRequest("big", 13400, (short) 1));
                assertEquals(Protocol.ERROR_RESPONSE, response.get(), "response type");
                assertTrue(Protocol.readString(response).startsWith("Too many partitions"), "error message");
                assertNull(zk.exists("/topics/big", false), "/topics/big in ZooKeeper");
            }
            assertCreated(slow);
        }
    }

    @Test
    void createRepliesAfterABrokerThatJoinedWhileItWaitedHasTheTopic() throws Exception {
        try (TestCluster cluster = TestCluster.start(2);
                ZooKeeper zk = cluster.openZooKeeper();
                SilentBroker silent = new SilentBroker().register(zk, 9)) {
            awaitBrokerCount(cluster, 1, 3);
            CompletableFuture<ByteBuffer> slow = createAsync(cluster, "slow", 1, (short) 1);
            awaitTopicInZooKeeper(zk, "slow");
            CompletableFuture<ByteBuffer> queued = createAsync(cluster, "queued", 4, (short) 1);
            // Nothing outside the controller shows that the create is queued, so give its request time to arrive.
            Thread.sleep(300);
            cluster.addBroker(3);

            assertCreated(queued);
            ByteBuffer produced = cluster.request(3, Protocol.encodeProduceRequest("queued", 0,
                    "m".getBytes(StandardCharsets.UTF_8)));
            assertNull(Protocol.decodeProduceResponse(produced).getError(), "produce to queued/0 through broker 3");
            assertCreated(slow);
        }
    }

    private static CompletableFuture<ByteBuffer> createAsync(TestCluster cluster, String topic, int partitions,
            short replicationFactor) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return cluster.request(1, Protocol.encodeCreateTopicRequest(topic, partitions, replicationFactor));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    private static void assertCreated(CompletableFuture<ByteBuffer> created) throws Exception {
        ByteBuffer response = created.get(30, TimeUnit.SECONDS);
        assertEquals(Protocol.CREATE_TOPIC_RESPONSE, response.get(), "response type");
        assertEquals(0, response.get(), "create status");
    }

    private static void awaitBrokerCount(TestCluster cluster, int brokerId, int count) {
        TestCluster.await("broker " + brokerId + " to list " + count + " brokers", () -> {
            try {
                return cluster.client(brokerId).getBrokers().size() == count;
            } catch (IOException e) {
                return false;
            }
        });
    }

    private static void awaitTopicInZooKeeper(ZooKeeper zk, String topic) {
        TestCluster.await(topic + " in ZooKeeper", () -> {
            try {
                return zk.exists("/topics/" + topic, false) != null;
            } catch (Exception e) {
                return false;
            }
        });
    }
}
