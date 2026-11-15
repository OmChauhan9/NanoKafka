package com.simplekafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import com.simplekafka.client.SimpleKafkaClient;

class TopicNameTest {

    @Test
    void aTopicNameCannotEscapeTheDataDirectory() throws Exception {
        Files.deleteIfExists(Path.of("escaped"));
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
            for (String name : List.of("", ".", "..", "a/b", "a\\b", "a b", "a\u0000b", "x".repeat(250),
                    "café", "가", "straße")) {
                assertFalse(client.createTopic(name, 1, (short) 1), "accepted invalid topic name: " + name);
            }
        }
    }

    /** Each pair differs as a string, but a case- and normalization-insensitive filesystem such as APFS maps both to one directory. */
    @Test
    void namesThatShareADirectoryNeverShareMessages() throws Exception {
        List<List<String>> pairs = List.of(
                List.of("가", "가"),
                List.of("豈", "豈"),
                List.of("straße", "strasse"),
                List.of("ﬀ", "ff"),
                List.of("ﬁx", "fix"));
        try (TestCluster cluster = TestCluster.start(1)) {
            SimpleKafkaClient client = cluster.client(1);
            for (List<String> pair : pairs) {
                List<String> created = new ArrayList<>();
                for (String name : pair) {
                    if (client.createTopic(name, 1, (short) 1)) {
                        created.add(name);
                    }
                }
                for (String name : created) {
                    assertEquals(0, client.send(name, 0, name.getBytes(StandardCharsets.UTF_8)), "send to " + name);
                }
                for (String name : created) {
                    List<byte[]> messages = client.fetch(name, 0, 0, 1 << 20);
                    assertEquals(List.of(name), messages.stream()
                            .map(m -> new String(m, StandardCharsets.UTF_8)).collect(Collectors.toList()),
                            "messages in topic " + name);
                }
            }
            for (String ascii : List.of("strasse", "ff", "fix")) {
                assertNotNull(client.getTopicMetadata(ascii), "ASCII topic " + ascii + " was created");
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
