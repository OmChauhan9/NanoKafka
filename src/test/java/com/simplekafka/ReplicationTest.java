package com.simplekafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import org.junit.jupiter.api.Test;

import com.simplekafka.broker.Protocol;
import com.simplekafka.broker.SimpleKafkaBroker;
import com.simplekafka.client.SimpleKafkaClient;

class ReplicationTest {

    @Test
    void followerReceivesACopyOfEachMessage() throws Exception {
        try (TestCluster cluster = TestCluster.start(2)) {
            SimpleKafkaClient client = cluster.client(1);
            client.createTopic("orders", 1, (short) 2);
            cluster.awaitTopicEverywhere("orders");
            SimpleKafkaClient.PartitionInfo partition = client.getTopicMetadata("orders").getPartitions().get(0);
            int follower = partition.getFollowers().get(0);

            client.send("orders", 0, "m0".getBytes(StandardCharsets.UTF_8));

            TestCluster.await("broker " + follower + " to hold m0", () -> {
                try {
                    List<byte[]> messages = TestCluster.messagesOnDisk(follower, "orders", 0);
                    return messages.size() == 1 && "m0".equals(new String(messages.get(0), StandardCharsets.UTF_8));
                } catch (Exception e) {
                    return false;
                }
            });
        }
    }

    @Test
    void produceSentToAFollowerIsForwardedToTheLeader() throws Exception {
        try (TestCluster cluster = TestCluster.start(2)) {
            SimpleKafkaClient client = cluster.client(1);
            client.createTopic("orders", 1, (short) 2);
            cluster.awaitTopicEverywhere("orders");
            SimpleKafkaClient.PartitionInfo partition = client.getTopicMetadata("orders").getPartitions().get(0);
            int follower = partition.getFollowers().get(0);

            Protocol.ProduceResult result = Protocol.decodeProduceResponse(cluster.request(follower,
                    Protocol.encodeProduceRequest("orders", 0, "m0".getBytes(StandardCharsets.UTF_8))));

            assertTrue(result.isSuccess(), "forwarded produce failed: " + result.getError());
            assertEquals(0, result.getOffset());
            assertEquals("m0", new String(client.fetch("orders", 0, 0, 1024).get(0), StandardCharsets.UTF_8));
        }
    }

    @Test
    void leaderReportsAFollowerCopyThatFailedToWrite() throws Exception {
        List<String> replicationLog = new CopyOnWriteArrayList<>();
        Handler handler = new Handler() {
            @Override
            public void publish(LogRecord record) {
                if (record.getMessage().startsWith("Replication to follower")) {
                    replicationLog.add(record.getMessage());
                }
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
        Logger logger = Logger.getLogger(SimpleKafkaBroker.class.getName());
        try (TestCluster cluster = TestCluster.start(2)) {
            SimpleKafkaClient client = cluster.client(1);
            client.createTopic("orders", 1, (short) 2);
            cluster.awaitTopicEverywhere("orders");
            int follower = client.getTopicMetadata("orders").getPartitions().get(0).getFollowers().get(0);
            File followerIndex = Path.of("data", String.valueOf(follower), "orders", "0",
                    "00000000000000000000.index").toFile();
            logger.addHandler(handler);
            try {
                assertTrue(followerIndex.setWritable(false), "made the follower's index read-only");

                client.send("orders", 0, "m0".getBytes(StandardCharsets.UTF_8));

                String prefix = "Replication to follower " + follower + " ";
                TestCluster.await("the leader to report the copy to broker " + follower,
                        () -> replicationLog.stream().anyMatch(line -> line.startsWith(prefix)));
                assertTrue(replicationLog.contains(prefix + "failed"), "leader log: " + replicationLog);
                assertFalse(replicationLog.contains(prefix + "succeeded"), "leader log: " + replicationLog);
            } finally {
                logger.removeHandler(handler);
                followerIndex.setWritable(true);
            }
        }
    }
}
