package com.simplekafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.simplekafka.client.SimpleKafkaClient;

class CreateTopicReadyTest {

    // Broker 1 starts first and becomes controller, so brokers 2 and 3 lead partitions they learn about
    // only through the controller's notification.
    @Test
    void everyPartitionAcceptsAProduceAsSoonAsCreateReturns() throws Exception {
        try (TestCluster cluster = TestCluster.start(3)) {
            SimpleKafkaClient client = cluster.client(1);
            List<String> failures = new ArrayList<>();
            for (int i = 0; i < 20; i++) {
                String topic = "t" + i;
                assertTrue(client.createTopic(topic, 3, (short) 2), "create failed for " + topic);
                for (int partition = 0; partition < 3; partition++) {
                    try {
                        client.send(topic, partition, "m".getBytes(StandardCharsets.UTF_8));
                    } catch (IOException e) {
                        failures.add(topic + "/" + partition + ": " + e.getMessage());
                    }
                }
            }
            assertEquals(List.of(), failures, "produces that failed right after createTopic returned true");
        }
    }
}
