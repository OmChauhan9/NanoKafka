package com.simplekafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Set;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.jupiter.api.Test;

import com.simplekafka.client.SimpleKafkaClient;

class StaleControllerSessionTest {

    @Test
    void brokerNamedByADeadControllerSessionKeepsServingThenTakesOver() throws Exception {
        try (TestCluster cluster = TestCluster.start(0)) {
            ZooKeeper deadSession = cluster.openZooKeeper();
            deadSession.create("/controller", "1".getBytes(StandardCharsets.UTF_8),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            cluster.addBroker(1);
            SimpleKafkaClient client = cluster.client(1);

            assertFalse(assertTimeoutPreemptively(Duration.ofSeconds(10),
                    () -> client.createTopic("early", 1, (short) 1)),
                    "create-topic fails while no live controller exists");
            assertEquals(Set.of(1), assertTimeoutPreemptively(Duration.ofSeconds(10),
                    () -> cluster.client(1).getBrokers().keySet()),
                    "broker 1 still serves metadata");

            deadSession.close();
            TestCluster.await("broker 1 to become controller and create a topic", () -> {
                try {
                    return client.createTopic("orders", 1, (short) 1);
                } catch (IOException e) {
                    return false;
                }
            });
            cluster.awaitTopicEverywhere("orders");
        }
    }
}
