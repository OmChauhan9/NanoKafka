package com.simplekafka;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.Test;

import com.simplekafka.client.SimpleKafkaClient;
import com.simplekafka.client.SimpleKafkaConsumer;

class ConsumerTest {

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
}
