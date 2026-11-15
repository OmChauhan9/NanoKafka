package com.simplekafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.simplekafka.broker.Protocol;
import com.simplekafka.client.SimpleKafkaClient;

class MetadataDuringTopicCreationTest {

    @Test
    void everyMetadataRequestGetsAWellFormedReplyWhileTopicsAreCreated() throws Exception {
        try (TestCluster cluster = TestCluster.start(1)) {
            SimpleKafkaClient admin = cluster.client(1);
            AtomicBoolean done = new AtomicBoolean();
            AtomicInteger replies = new AtomicInteger();
            List<String> failures = Collections.synchronizedList(new ArrayList<>());
            List<Thread> readers = new ArrayList<>();
            for (int r = 0; r < 2; r++) {
                Thread reader = new Thread(() -> {
                    while (!done.get()) {
                        try {
                            Protocol.MetadataResult result = Protocol.decodeMetadataResponse(
                                    cluster.request(1, Protocol.encodeMetadataRequest()));
                            if (!result.isSuccess()) {
                                failures.add("error reply: " + result.getError());
                            }
                            replies.incrementAndGet();
                            Thread.sleep(5);
                        } catch (InterruptedException e) {
                            return;
                        } catch (Exception e) {
                            failures.add(e.toString());
                        }
                    }
                });
                reader.start();
                readers.add(reader);
            }

            try {
                for (int i = 0; i < 600; i++) {
                    assertTrue(admin.createTopic("t" + i, 1, (short) 1), "create failed for t" + i);
                }
            } finally {
                done.set(true);
                for (Thread reader : readers) {
                    reader.join();
                }
            }

            assertEquals(List.of(), failures, "metadata requests that got no well-formed reply out of "
                    + (replies.get() + failures.size()));
        }
    }
}
