package com.simplekafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

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
}
