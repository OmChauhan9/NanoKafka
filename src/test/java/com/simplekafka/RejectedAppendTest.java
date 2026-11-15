package com.simplekafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;

import com.simplekafka.broker.Protocol;
import com.simplekafka.client.SimpleKafkaClient;

class RejectedAppendTest {

    @Test
    void aMessageTheLeaderRejectsNeverReachesAFollower() throws Exception {
        try (TestCluster cluster = TestCluster.start(2)) {
            SimpleKafkaClient client = cluster.client(1);
            client.createTopic("events", 1, (short) 2);
            cluster.awaitTopicEverywhere("events");
            SimpleKafkaClient.PartitionInfo partition = client.getTopicMetadata("events").getPartitions().get(0);
            int follower = partition.getFollowers().get(0);
            Path index = Path.of("data", String.valueOf(partition.getLeader()), "events", "0",
                    "00000000000000000000.index");

            assertTrue(index.toFile().setWritable(false), "could not make the leader's index read-only");
            try {
                assertThrows(IOException.class,
                        () -> client.send("events", 0, "rejected".getBytes(StandardCharsets.UTF_8)));
            } finally {
                index.toFile().setWritable(true);
            }

            // Replication is asynchronous, so give a wrongly sent copy time to arrive before checking.
            Thread.sleep(1000);
            assertEquals(0, messagesOn(cluster, follower), "follower stored a message its leader rejected");
        }
    }

    private static int messagesOn(TestCluster cluster, int brokerId) throws IOException {
        return Protocol.decodeFetchResponse(
                cluster.request(brokerId, Protocol.encodeFetchRequest("events", 0, 0, 1024))).getMessageCount();
    }
}
