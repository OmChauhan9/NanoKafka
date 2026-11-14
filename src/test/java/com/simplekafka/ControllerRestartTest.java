package com.simplekafka;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import com.simplekafka.client.SimpleKafkaClient;

class ControllerRestartTest {

    @Test
    void restartedControllerMovesLoadedPartitionsOffDeadBrokers() throws Exception {
        try (TestCluster cluster = TestCluster.start(2)) {
            cluster.client(1).createTopic("orders", 2, (short) 1);
            cluster.awaitTopicEverywhere("orders");

            cluster.stopBroker(1);
            cluster.stopBroker(2);
            cluster.restartBroker(1);

            TestCluster.await("broker 1 to lead every partition of orders", () -> {
                try {
                    SimpleKafkaClient.TopicMetadata orders = cluster.client(1).getTopicMetadata("orders");
                    return orders != null && orders.getPartitions().stream()
                            .map(SimpleKafkaClient.PartitionInfo::getLeader)
                            .collect(Collectors.toSet())
                            .equals(Set.of(1));
                } catch (IOException e) {
                    return false;
                }
            });
            assertEquals(0, cluster.client(1).send("orders", 1, "m".getBytes(StandardCharsets.UTF_8)));
        }
    }
}
