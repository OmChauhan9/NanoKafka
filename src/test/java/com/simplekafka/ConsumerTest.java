package com.simplekafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.Test;

import com.simplekafka.client.SimpleKafkaClient;
import com.simplekafka.client.SimpleKafkaConsumer;

class ConsumerTest {

    @Test
    void seekMovesTheNextPollToTheGivenOffset() throws Exception {
        try (TestCluster cluster = TestCluster.start(1)) {
            SimpleKafkaClient client = cluster.client(1);
            client.createTopic("events", 1, (short) 1);
            cluster.awaitTopicEverywhere("events");
            for (int i = 0; i < 3; i++) {
                client.send("events", 0, ("m" + i).getBytes(StandardCharsets.UTF_8));
            }

            SimpleKafkaConsumer consumer = new SimpleKafkaConsumer("localhost", cluster.port(1), "events", 0);
            consumer.initialize();

            consumer.seek(2);
            assertEquals(List.of("m2"), strings(consumer.poll()));
            consumer.seek(0);
            assertEquals(List.of("m0", "m1", "m2"), strings(consumer.poll()));
            assertEquals(3, consumer.getCurrentOffset());
        }
    }

    private static List<String> strings(List<byte[]> messages) {
        return messages.stream().map(m -> new String(m, StandardCharsets.UTF_8)).collect(java.util.stream.Collectors.toList());
    }

    @Test
    void restartingAfterAHandlerFailureRedeliversTheFailedMessageAndTheRest() throws Exception {
        try (TestCluster cluster = TestCluster.start(1)) {
            SimpleKafkaClient client = cluster.client(1);
            client.createTopic("events", 1, (short) 1);
            cluster.awaitTopicEverywhere("events");
            for (int i = 0; i < 5; i++) {
                client.send("events", 0, ("m" + i).getBytes(StandardCharsets.UTF_8));
            }

            SimpleKafkaConsumer consumer = new SimpleKafkaConsumer("localhost", cluster.port(1), "events", 0);
            consumer.initialize();
            List<String> delivered = new CopyOnWriteArrayList<>();

            consumer.startConsuming((message, offset) -> {
                if (offset == 1) {
                    throw new IllegalStateException("handler failed on offset 1");
                }
                delivered.add(offset + ":" + new String(message, StandardCharsets.UTF_8));
            });
            TestCluster.await("consumer to stop after the handler throws", () -> !consumer.isRunning());
            assertEquals(List.of("0:m0"), delivered);
            assertEquals(1, consumer.getCurrentOffset());

            consumer.startConsuming((message, offset) ->
                    delivered.add(offset + ":" + new String(message, StandardCharsets.UTF_8)));
            TestCluster.await("remaining messages delivered", () -> delivered.size() == 5);
            consumer.close();

            assertEquals(List.of("0:m0", "1:m1", "2:m2", "3:m3", "4:m4"), delivered);
            assertEquals(5, consumer.getCurrentOffset());
        }
    }

    @Test
    void aRunningConsumerResumesAfterItsBrokerRestarts() throws Exception {
        try (TestCluster cluster = TestCluster.start(1)) {
            SimpleKafkaClient client = cluster.client(1);
            client.createTopic("events", 1, (short) 1);
            cluster.awaitTopicEverywhere("events");
            client.send("events", 0, "m0".getBytes(StandardCharsets.UTF_8));
            client.send("events", 0, "m1".getBytes(StandardCharsets.UTF_8));

            SimpleKafkaConsumer consumer = new SimpleKafkaConsumer("localhost", cluster.port(1), "events", 0);
            consumer.initialize();
            List<String> delivered = new CopyOnWriteArrayList<>();
            consumer.startConsuming((message, offset) ->
                    delivered.add(offset + ":" + new String(message, StandardCharsets.UTF_8)));
            TestCluster.await("messages before the restart", () -> delivered.size() == 2);

            cluster.stopBroker(1);
            // The outage has no observable end on the consumer side, so hold it for several poll intervals.
            Thread.sleep(1000);
            cluster.restartBroker(1);
            SimpleKafkaClient fresh = cluster.client(1);
            fresh.send("events", 0, "m2".getBytes(StandardCharsets.UTF_8));
            fresh.send("events", 0, "m3".getBytes(StandardCharsets.UTF_8));

            TestCluster.await("messages after the restart", () -> delivered.size() == 4 || !consumer.isRunning());
            assertTrue(consumer.isRunning(), "consumer stopped during the broker restart");
            consumer.close();
            assertEquals(List.of("0:m0", "1:m1", "2:m2", "3:m3"), delivered);
        }
    }
}
