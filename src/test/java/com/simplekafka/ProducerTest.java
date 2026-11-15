package com.simplekafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

import com.simplekafka.client.SimpleKafkaClient;
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

    @Test
    void spreadsMessagesAcrossPartitions() throws Exception {
        try (TestCluster cluster = TestCluster.start(1)) {
            SimpleKafkaProducer producer = new SimpleKafkaProducer("localhost", cluster.port(1), "spread");
            producer.initialize();
            for (int i = 0; i < 30; i++) {
                producer.send("m" + i);
            }

            SimpleKafkaClient client = cluster.client(1);
            int partitionsUsed = 0;
            int total = 0;
            for (int partition = 0; partition < 3; partition++) {
                int count = client.fetch("spread", partition, 0, 1024 * 1024).size();
                total += count;
                if (count > 0) {
                    partitionsUsed++;
                }
            }
            assertEquals(30, total);
            // A random choice puts all 30 messages on one of 3 partitions with probability 3 / 3^30.
            assertTrue(partitionsUsed > 1, "all 30 messages went to one partition");
        }
    }
}
