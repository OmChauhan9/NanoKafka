package com.simplekafka;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import com.simplekafka.client.SimpleKafkaClient;

class ProduceFetchTest {

    @Test
    void messagesComeBackInOrderAtTheirOffsets() throws Exception {
        try (TestCluster cluster = TestCluster.start(2)) {
            SimpleKafkaClient client = cluster.client(1);
            client.createTopic("orders", 2, (short) 2);
            cluster.awaitTopicEverywhere("orders");

            for (int i = 0; i < 3; i++) {
                assertEquals(i, client.send("orders", 1, ("m" + i).getBytes(StandardCharsets.UTF_8)));
            }

            List<String> fetched = client.fetch("orders", 1, 0, 1024).stream()
                    .map(bytes -> new String(bytes, StandardCharsets.UTF_8))
                    .collect(Collectors.toList());
            assertEquals(List.of("m0", "m1", "m2"), fetched);
            assertEquals(List.of("m2"), client.fetch("orders", 1, 2, 1024).stream()
                    .map(bytes -> new String(bytes, StandardCharsets.UTF_8))
                    .collect(Collectors.toList()));
        }
    }
}
