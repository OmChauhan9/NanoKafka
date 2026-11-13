package com.simplekafka;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

import com.simplekafka.client.SimpleKafkaProducer;

class ProducerTest {

    @Test
    void autoCreatesItsTopicOnASingleBroker() throws Exception {
        try (TestCluster cluster = TestCluster.start(1)) {
            SimpleKafkaProducer producer = new SimpleKafkaProducer("localhost", cluster.port(1), "events");
            producer.initialize();

            assertEquals(0, producer.send("first", 0));
            assertEquals("first", new String(cluster.client(1).fetch("events", 0, 0, 1024).get(0),
                    StandardCharsets.UTF_8));
        }
    }
}
