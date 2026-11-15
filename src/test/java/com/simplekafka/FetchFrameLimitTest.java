package com.simplekafka;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.simplekafka.broker.Protocol;
import com.simplekafka.client.SimpleKafkaClient;

class FetchFrameLimitTest {

    @Test
    void fetchWithMaxBytesAboveTheFrameLimitReturnsPrefixesThatFit() throws Exception {
        try (TestCluster cluster = TestCluster.start(1)) {
            SimpleKafkaClient client = cluster.client(1);
            client.createTopic("big", 1, (short) 1);
            // 16 of these fill a 16 MB read budget exactly, but not a 16 MB frame with fetch overhead.
            int count = 17;
            for (int i = 0; i < count; i++) {
                client.send("big", 0, messageOf(i));
            }

            int offset = 0;
            while (offset < count) {
                List<byte[]> batch = client.fetch("big", 0, offset, 32 << 20);
                assertFalse(batch.isEmpty(), "fetch from offset " + offset + " made no progress");
                for (byte[] message : batch) {
                    assertArrayEquals(messageOf(offset), message, "message at offset " + offset);
                    offset++;
                }
            }
            assertEquals(count, offset);
        }
    }

    @Test
    void produceAcceptsOnlyMessagesAFetchCanReturn() throws Exception {
        try (TestCluster cluster = TestCluster.start(1)) {
            SimpleKafkaClient client = cluster.client(1);
            client.createTopic("t", 1, (short) 1);

            byte[] largest = new byte[Protocol.maxMessageBytes("t")];
            largest[0] = 1;
            largest[largest.length - 1] = 2;
            assertEquals(0, client.send("t", 0, largest));
            List<byte[]> fetched = client.fetch("t", 0, 0, 1);
            assertEquals(1, fetched.size());
            assertArrayEquals(largest, fetched.get(0));

            assertThrows(IOException.class, () -> client.send("t", 0, new byte[Protocol.maxMessageBytes("t") + 1]),
                    "produce of a message one byte larger than any fetch can return");
            assertEquals(0, client.fetch("t", 0, 1, 1).size(), "rejected message was not appended");
        }
    }

    @Test
    void produceAcceptsOnlyMessagesThatCanReachEveryFollower() throws Exception {
        try (TestCluster cluster = TestCluster.start(2)) {
            SimpleKafkaClient client = cluster.client(1);
            client.createTopic("tt", 1, (short) 2);
            cluster.awaitTopicEverywhere("tt");
            int follower = client.getTopicMetadata("tt").getPartitions().get(0).getFollowers().get(0);
            // A replicate frame carries 19 bytes plus the topic name around the message.
            int largestReplicable = Protocol.MAX_FRAME_BYTES - 19 - "tt".length();

            byte[] largest = new byte[largestReplicable];
            largest[largest.length - 1] = 7;
            assertEquals(0, client.send("tt", 0, largest));
            TestCluster.await("follower " + follower + " to hold the largest message", () -> {
                try {
                    List<byte[]> messages = TestCluster.messagesOnDisk(follower, "tt", 0);
                    return messages.size() == 1 && Arrays.equals(largest, messages.get(0));
                } catch (IOException e) {
                    return false;
                }
            });

            assertThrows(IOException.class, () -> client.send("tt", 0, new byte[largestReplicable + 1]),
                    "produce of a message one byte larger than a replicate frame can carry");
        }
    }

    private static byte[] messageOf(int value) {
        byte[] message = new byte[(1 << 20) - 4];
        Arrays.fill(message, (byte) value);
        return message;
    }
}
