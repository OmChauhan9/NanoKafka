package com.simplekafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.simplekafka.client.SimpleKafkaClient;

class TopicNameTest {

    @Test
    void aTopicNameCannotEscapeTheDataDirectory() throws Exception {
        try (TestCluster cluster = TestCluster.start(1)) {
            assertFalse(cluster.client(1).createTopic("../../escaped", 1, (short) 1));

            assertFalse(Files.exists(Path.of("escaped")), "created a directory outside the data dir");
            assertFalse(Files.exists(Path.of("data", "escaped")), "created a directory outside the broker's dir");
        }
    }

    @Test
    void invalidTopicNamesAreRejected() throws Exception {
        try (TestCluster cluster = TestCluster.start(1)) {
            SimpleKafkaClient client = cluster.client(1);
            for (String name : List.of("", ".", "..", "a/b", "a\\b", "a b", "a\u0000b", "x".repeat(250))) {
                assertFalse(client.createTopic(name, 1, (short) 1), "accepted invalid topic name: " + name);
            }
        }
    }

    @Test
    void validTopicNamesStillWork() throws Exception {
        try (TestCluster cluster = TestCluster.start(1)) {
            SimpleKafkaClient client = cluster.client(1);
            for (String name : List.of("orders.v1_2-x", "x".repeat(249))) {
                assertTrue(client.createTopic(name, 1, (short) 1), "rejected valid topic name: " + name);
                assertEquals(0, client.send(name, 0, "m".getBytes(StandardCharsets.UTF_8)));
            }
        }
    }
}
