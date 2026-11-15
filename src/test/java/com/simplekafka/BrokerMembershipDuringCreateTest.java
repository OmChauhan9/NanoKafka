package com.simplekafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.ZooKeeper;
import org.junit.jupiter.api.Test;

import com.simplekafka.broker.Protocol;

class BrokerMembershipDuringCreateTest {

    // A broker that starts while the controller builds a topic reads ZooKeeper before the topic is there, and the
    // controller picked the brokers it notifies before the new broker registered.
    @Test
    void brokerThatJoinsDuringCreateLearnsTheTopic() throws Exception {
        try (TestCluster cluster = TestCluster.start(2);
                ZooKeeper zk = cluster.openZooKeeper()) {
            CompletableFuture<ByteBuffer> created = createAsync(cluster);
            TestCluster.await("the controller to start building wide",
                    () -> Files.isDirectory(Path.of("data", "1", "wide")));
            cluster.addBroker(3);
            assertCreated(created);

            assertEquals(List.of(), cluster.awaitAgreementWithZooKeeper(zk, "wide", 1, 2, 3),
                    "brokers whose metadata for wide differs from ZooKeeper");
            ByteBuffer produced = cluster.request(3, Protocol.encodeProduceRequest("wide", 399,
                    "m".getBytes(StandardCharsets.UTF_8)));
            assertNull(Protocol.decodeProduceResponse(produced).getError(), "produce to partition 399 through broker 3");
        }
    }

    // Broker 3 stops and starts again while the controller tells brokers about a new topic. Its first session's
    // partitions move to their followers, so the restarted broker must not keep leading them.
    @Test
    void brokerThatRestartsDuringCreateAgreesWithZooKeeperOnEveryLeader() throws Exception {
        try (TestCluster cluster = TestCluster.start(3);
                ZooKeeper zk = cluster.openZooKeeper()) {
            CompletableFuture<ByteBuffer> created = createAsync(cluster);
            TestCluster.await("wide in ZooKeeper", () -> {
                try {
                    return zk.exists("/topics/wide/partitions/20", false) != null;
                } catch (Exception e) {
                    return false;
                }
            });
            cluster.stopBroker(3);
            cluster.restartBroker(3);
            assertCreated(created);

            assertEquals(List.of(), cluster.awaitAgreementWithZooKeeper(zk, "wide", 1, 2, 3),
                    "brokers whose metadata for wide differs from ZooKeeper");
        }
    }

    private static CompletableFuture<ByteBuffer> createAsync(TestCluster cluster) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return cluster.request(1, Protocol.encodeCreateTopicRequest("wide", 400, (short) 2));
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
}
