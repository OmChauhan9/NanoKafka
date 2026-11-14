package com.simplekafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.simplekafka.client.SimpleKafkaClient;

class CreateTopicVisibilityTest {

    // Broker 1 starts first and becomes controller, so a client bootstrapped at broker 2 reads metadata
    // from a broker that learns about new topics only through the controller's notification.
    @Test
    void aCreatedTopicIsInTheClientsMetadataAsSoonAsCreateReturns() throws Exception {
        try (TestCluster cluster = TestCluster.start(2)) {
            SimpleKafkaClient client = cluster.client(2);
            List<String> missing = new ArrayList<>();
            for (int i = 0; i < 20; i++) {
                String topic = "t" + i;
                assertTrue(client.createTopic(topic, 3, (short) 2), "create failed for " + topic);
                if (client.getTopicMetadata(topic) == null) {
                    missing.add(topic);
                }
            }
            assertEquals(List.of(), missing, "topics missing from metadata right after createTopic returned true");
        }
    }
}
