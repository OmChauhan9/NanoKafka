package com.simplekafka;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.simplekafka.broker.Protocol;
import com.simplekafka.client.SimpleKafkaClient;
import com.simplekafka.client.SimpleKafkaClient.PartitionInfo;
import com.simplekafka.client.SimpleKafkaConsumer;

class LeaderFailoverTest {

    @Test
    void aClientWithCachedMetadataFollowsTheNewLeader() throws Exception {
        try (TestCluster cluster = TestCluster.start(2)) {
            SimpleKafkaClient admin = cluster.client(1);
            admin.createTopic("orders", 2, (short) 2);
            cluster.awaitTopicEverywhere("orders");
            int partition = partitionLedBy(admin, "orders", 2);

            SimpleKafkaClient producer = cluster.client(1);
            SimpleKafkaClient consumer = cluster.client(1);
            producer.send("orders", partition, "before".getBytes(StandardCharsets.UTF_8));

            cluster.stopBroker(2);
            TestCluster.await("broker 1 to lead partition " + partition, () -> {
                try {
                    return leaderOf(cluster.client(1), "orders", partition) == 1;
                } catch (IOException e) {
                    return false;
                }
            });

            assertDoesNotThrow(() -> producer.send("orders", partition, "after".getBytes(StandardCharsets.UTF_8)));
            assertDoesNotThrow(() -> consumer.fetch("orders", partition, 0, 1024));
        }
    }

    // Broker 2 comes back as a follower that missed every message, while both readers still name it leader.
    @Test
    void aReaderWithCachedMetadataReadsFromTheNewLeaderAfterTheOldOneReturns() throws Exception {
        try (TestCluster cluster = TestCluster.start(2)) {
            SimpleKafkaClient admin = cluster.client(1);
            admin.createTopic("orders", 2, (short) 2);
            cluster.awaitTopicEverywhere("orders");
            int partition = partitionLedBy(admin, "orders", 2);

            SimpleKafkaClient client = cluster.client(1);
            SimpleKafkaConsumer consumer = new SimpleKafkaConsumer("localhost", cluster.port(1), "orders", partition);
            consumer.initialize();

            cluster.stopBroker(2);
            TestCluster.await("broker 1 to lead partition " + partition, () -> {
                try {
                    return leaderOf(cluster.client(1), "orders", partition) == 1;
                } catch (IOException e) {
                    return false;
                }
            });
            admin.refreshMetadata();
            admin.send("orders", partition, "m1".getBytes(StandardCharsets.UTF_8));
            admin.send("orders", partition, "m2".getBytes(StandardCharsets.UTF_8));
            cluster.restartBroker(2);
            TestCluster.await("broker 2 to name broker 1 leader of partition " + partition, () -> {
                try {
                    return leaderOf(cluster.client(2), "orders", partition) == 1;
                } catch (IOException e) {
                    return false;
                }
            });

            assertEquals(List.of("m1", "m2"), strings(consumer.poll()), "consumer poll");
            assertEquals(List.of("m1", "m2"), strings(client.fetch("orders", partition, 0, 1 << 20)), "client fetch");
        }
    }

    private static List<String> strings(List<byte[]> messages) {
        List<String> out = new ArrayList<>();
        for (byte[] message : messages) {
            out.add(new String(message, StandardCharsets.UTF_8));
        }
        return out;
    }

    // Broker 1 starts first and becomes controller, so it is neither the old nor the new leader here.
    @Test
    void theSurvivingReplicaTakesOverAndEveryBrokerRoutesToIt() throws Exception {
        try (TestCluster cluster = TestCluster.start(3)) {
            SimpleKafkaClient admin = cluster.client(1);
            admin.createTopic("orders", 3, (short) 2);
            cluster.awaitTopicEverywhere("orders");
            int partition = partitionLedBy(admin, "orders", 2);
            assertEquals(List.of(3), partitionInfo(admin, "orders", partition).getFollowers());

            List<String> sent = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                sent.add("m" + i);
                admin.send("orders", partition, ("m" + i).getBytes(StandardCharsets.UTF_8));
            }
            TestCluster.await("broker 3 to hold all 5 messages", () -> {
                try {
                    return TestCluster.messagesOnDisk(3, "orders", partition).size() == 5;
                } catch (IOException e) {
                    return false;
                }
            });

            cluster.stopBroker(2);
            TestCluster.await("brokers 1 and 3 to name broker 3 leader of partition " + partition, () -> {
                try {
                    return leaderOf(cluster.client(1), "orders", partition) == 3
                            && leaderOf(cluster.client(3), "orders", partition) == 3;
                } catch (IOException e) {
                    return false;
                }
            });

            for (int broker : new int[] {1, 3}) {
                PartitionInfo info = partitionInfo(cluster.client(broker), "orders", partition);
                assertEquals(Set.of(2, 3), replicasOf(info), "broker " + broker + " reports " + info);
            }
            List<String> fetched = new ArrayList<>();
            for (byte[] message : cluster.client(1).fetch("orders", partition, 0, 1 << 20)) {
                fetched.add(new String(message, StandardCharsets.UTF_8));
            }
            assertEquals(sent, fetched);
            for (int broker : new int[] {1, 3}) {
                Protocol.ProduceResult result = Protocol.decodeProduceResponse(cluster.request(broker,
                        Protocol.encodeProduceRequest("orders", partition, "after".getBytes(StandardCharsets.UTF_8))));
                assertTrue(result.isSuccess(), "produce through broker " + broker + " failed: " + result.getError());
            }
        }
    }

    @Test
    void aPartitionWithNoSurvivingReplicaWaitsLeaderlessForItToReturn() throws Exception {
        try (TestCluster cluster = TestCluster.start(3)) {
            SimpleKafkaClient admin = cluster.client(1);
            admin.createTopic("single", 3, (short) 1);
            cluster.awaitTopicEverywhere("single");
            int partition = partitionLedBy(admin, "single", 2);

            cluster.stopBroker(2);
            TestCluster.await("brokers 1 and 3 to report partition " + partition + " leaderless", () -> {
                try {
                    return leaderOf(cluster.client(1), "single", partition) == -1
                            && leaderOf(cluster.client(3), "single", partition) == -1;
                } catch (IOException e) {
                    return false;
                }
            });
            for (int broker : new int[] {1, 3}) {
                for (PartitionInfo info : cluster.client(broker).getTopicMetadata("single").getPartitions()) {
                    Set<Integer> expected = info.getId() == partition ? Set.of(2) : Set.of(info.getLeader());
                    assertEquals(expected, replicasOf(info), "broker " + broker + " reports " + info);
                }
            }

            cluster.restartBroker(2);
            TestCluster.await("broker 2 to lead partition " + partition + " again on every broker", () -> {
                try {
                    for (int broker = 1; broker <= 3; broker++) {
                        if (leaderOf(cluster.client(broker), "single", partition) != 2) {
                            return false;
                        }
                    }
                    return true;
                } catch (IOException e) {
                    return false;
                }
            });
        }
    }

    private static int partitionLedBy(SimpleKafkaClient client, String topic, int brokerId) {
        for (PartitionInfo info : client.getTopicMetadata(topic).getPartitions()) {
            if (info.getLeader() == brokerId) {
                return info.getId();
            }
        }
        throw new AssertionError("no partition of " + topic + " led by broker " + brokerId);
    }

    private static Set<Integer> replicasOf(PartitionInfo info) {
        Set<Integer> replicas = new HashSet<>(info.getFollowers());
        if (info.getLeader() != -1) {
            replicas.add(info.getLeader());
        }
        return replicas;
    }

    private static int leaderOf(SimpleKafkaClient client, String topic, int partition) {
        return partitionInfo(client, topic, partition).getLeader();
    }

    private static PartitionInfo partitionInfo(SimpleKafkaClient client, String topic, int partition) {
        return client.getTopicMetadata(topic).getPartitions().stream()
                .filter(info -> info.getId() == partition)
                .findFirst()
                .orElseThrow();
    }
}
