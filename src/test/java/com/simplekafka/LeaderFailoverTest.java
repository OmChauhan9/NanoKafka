package com.simplekafka;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

import com.simplekafka.client.SimpleKafkaClient;
import com.simplekafka.client.SimpleKafkaClient.PartitionInfo;

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

    private static int partitionLedBy(SimpleKafkaClient client, String topic, int brokerId) {
        for (PartitionInfo info : client.getTopicMetadata(topic).getPartitions()) {
            if (info.getLeader() == brokerId) {
                return info.getId();
            }
        }
        throw new AssertionError("no partition of " + topic + " led by broker " + brokerId);
    }

    private static int leaderOf(SimpleKafkaClient client, String topic, int partition) {
        return client.getTopicMetadata(topic).getPartitions().get(partition).getLeader();
    }
}
