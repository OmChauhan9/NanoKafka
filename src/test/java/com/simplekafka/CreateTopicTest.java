package com.simplekafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.apache.zookeeper.ZooKeeper;
import org.junit.jupiter.api.Test;

import com.simplekafka.broker.Protocol;
import com.simplekafka.client.SimpleKafkaClient;

class CreateTopicTest {

    @Test
    void createReportsFailureWhenZooKeeperRejectsTheTopic() throws Exception {
        try (TestCluster cluster = TestCluster.start(1)) {
            assertFalse(cluster.client(1).createTopic("a/b", 1, (short) 1),
                    "createTopic must report the ZooKeeper failure");
        }
    }

    @Test
    void concurrentCreatesOfOneTopicLetExactlyOneSucceed() throws Exception {
        int creators = 4;
        ExecutorService pool = Executors.newFixedThreadPool(creators);
        try (TestCluster cluster = TestCluster.start(1)) {
            CountDownLatch go = new CountDownLatch(1);
            List<Future<Boolean>> results = new ArrayList<>();
            for (int i = 0; i < creators; i++) {
                SimpleKafkaClient client = cluster.client(1);
                Callable<Boolean> create = () -> {
                    go.await();
                    return client.createTopic("race", 100, (short) 1);
                };
                results.add(pool.submit(create));
            }
            go.countDown();
            int succeeded = 0;
            for (Future<Boolean> result : results) {
                if (result.get()) {
                    succeeded++;
                }
            }
            assertEquals(1, succeeded, "creates of one topic that reported success");

            SimpleKafkaClient client = cluster.client(1);
            assertEquals(0, client.send("race", 0, "first".getBytes(StandardCharsets.UTF_8)));
            assertEquals(1, client.send("race", 0, "second".getBytes(StandardCharsets.UTF_8)));
            assertEquals(List.of("first", "second"), client.fetch("race", 0, 0, 1024).stream()
                    .map(bytes -> new String(bytes, StandardCharsets.UTF_8))
                    .collect(Collectors.toList()));
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void createRejectsANameThatDiffersFromAnExistingTopicOnlyInCase() throws Exception {
        try (TestCluster cluster = TestCluster.start(1)) {
            SimpleKafkaClient client = cluster.client(1);
            assertTrue(client.createTopic("demo", 1, (short) 1));
            assertEquals(0, client.send("demo", 0, "kept".getBytes(StandardCharsets.UTF_8)));

            assertFalse(client.createTopic("DEMO", 1, (short) 1), "create of DEMO after demo");
            assertFalse(client.createTopic("Demo", 1, (short) 1), "create of Demo after demo");

            assertEquals(List.of("kept"), client.fetch("demo", 0, 0, 1024).stream()
                    .map(bytes -> new String(bytes, StandardCharsets.UTF_8))
                    .collect(Collectors.toList()));
            assertTrue(client.createTopic("demo2", 1, (short) 1), "create of a distinct name");
        }
    }

    @Test
    void createTooLargeForOneZooKeeperRequestIsRefusedAndWritesNothing() throws Exception {
        try (TestCluster cluster = TestCluster.start(1);
                ZooKeeper zk = cluster.openZooKeeper()) {
            ByteBuffer response = cluster.request(1, Protocol.encodeCreateTopicRequest("huge", 20000, (short) 1));
            assertEquals(Protocol.ERROR_RESPONSE, response.get(), "response type");
            assertTrue(Protocol.readString(response).startsWith("Too many partitions"), "error message");
            assertNull(zk.exists("/topics/huge", false), "/topics/huge in ZooKeeper");
        }
    }
}
