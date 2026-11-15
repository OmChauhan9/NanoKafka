package com.simplekafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.junit.jupiter.api.Test;

import com.simplekafka.broker.Protocol;
import com.simplekafka.client.SimpleKafkaClient;

class CreateTopicTest {

    @Test
    void createReportsFailureWhenZooKeeperRejectsTheTopic() throws Exception {
        try (TestCluster cluster = TestCluster.start(1);
                ZooKeeper zk = cluster.openZooKeeper()) {
            SimpleKafkaClient client = cluster.client(1);
            // ADMIN stays so this session can put the open ACL back.
            List<ACL> readOnly = Collections.singletonList(
                    new ACL(ZooDefs.Perms.READ | ZooDefs.Perms.ADMIN, ZooDefs.Ids.ANYONE_ID_UNSAFE));
            zk.setACL("/topics", readOnly, -1);

            assertFalse(client.createTopic("rejected", 1, (short) 1), "createTopic must report the ZooKeeper failure");

            zk.setACL("/topics", ZooDefs.Ids.OPEN_ACL_UNSAFE, -1);
            assertTrue(client.createTopic("rejected", 1, (short) 1), "retry after ZooKeeper accepts writes again");
            assertEquals(0, client.send("rejected", 0, "m".getBytes(StandardCharsets.UTF_8)));
        }
    }

    @Test
    void concurrentCreatesOfOneTopicLetExactlyOneSucceed() throws Exception {
        try (TestCluster cluster = TestCluster.start(1)) {
            List<Boolean> results = createAllAtOnce(cluster, List.of("race", "race", "race", "race"), 100);
            assertEquals(1, results.stream().filter(created -> created).count(), "creates that reported success");

            SimpleKafkaClient client = cluster.client(1);
            assertEquals(0, client.send("race", 0, "first".getBytes(StandardCharsets.UTF_8)));
            assertEquals(1, client.send("race", 0, "second".getBytes(StandardCharsets.UTF_8)));
            assertEquals(List.of("first", "second"), client.fetch("race", 0, 0, 1024).stream()
                    .map(bytes -> new String(bytes, StandardCharsets.UTF_8))
                    .collect(Collectors.toList()));
        }
    }

    @Test
    void concurrentCreatesOfNamesThatDifferOnlyInCaseLetExactlyOneSucceed() throws Exception {
        List<String> names = List.of("Orders", "orders", "ORDERS", "oRdErS");
        try (TestCluster cluster = TestCluster.start(1);
                ZooKeeper zk = cluster.openZooKeeper()) {
            List<Boolean> results = createAllAtOnce(cluster, names, 1);

            assertEquals(1, results.stream().filter(created -> created).count(),
                    "creates that reported success " + results);
            List<String> stored = zk.getChildren("/topics", false);
            assertEquals(1, stored.stream().filter("orders"::equalsIgnoreCase).count(),
                    "topics in ZooKeeper " + stored);
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

    private static List<Boolean> createAllAtOnce(TestCluster cluster, List<String> names, int partitions)
            throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(names.size());
        try {
            CountDownLatch go = new CountDownLatch(1);
            List<Future<Boolean>> pending = new ArrayList<>();
            for (String name : names) {
                SimpleKafkaClient client = cluster.client(1);
                pending.add(pool.submit(() -> {
                    go.await();
                    return client.createTopic(name, partitions, (short) 1);
                }));
            }
            go.countDown();
            List<Boolean> results = new ArrayList<>();
            for (Future<Boolean> result : pending) {
                results.add(result.get());
            }
            return results;
        } finally {
            pool.shutdownNow();
        }
    }
}
