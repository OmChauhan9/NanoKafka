package com.simplekafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

import com.simplekafka.broker.Protocol;
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
                    Protocol.FetchResult result = Protocol.decodeFetchResponse(
                            cluster.request(follower, Protocol.encodeFetchRequest("orders", 0, 0, 1024)));
                    return result.getMessageCount() == 1
                            && "m0".equals(new String(result.getMessages()[0], StandardCharsets.UTF_8));
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
}
