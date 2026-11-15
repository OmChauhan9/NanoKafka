package com.simplekafka;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.jupiter.api.Test;

import com.simplekafka.broker.Protocol;
import com.simplekafka.client.SimpleKafkaClient;

class PartialTopicLoadTest {

    // A broker that loads a topic while the controller is still writing its partition nodes sees only some
    // partitions, so a later notification must add the ones it missed.
    @Test
    void notificationAddsPartitionsMissingFromAnEarlierLoad() throws Exception {
        try (TestCluster cluster = TestCluster.start(1);
                ZooKeeper zk = cluster.openZooKeeper()) {
            createNode(zk, "/topics/partial");
            createNode(zk, "/topics/partial/partitions");
            createPartitions(zk, 0, 2);
            assertEquals(0, cluster.request(1, Protocol.encodeTopicNotification("partial")).get(), "first load acked");

            createPartitions(zk, 2, 6);
            assertEquals(0, cluster.request(1, Protocol.encodeTopicNotification("partial")).get(), "reload acked");

            SimpleKafkaClient client = cluster.client(1);
            assertEquals(6, client.getTopicMetadata("partial").getPartitions().size(), "partitions broker 1 serves");
            List<String> failures = new ArrayList<>();
            for (int partition = 0; partition < 6; partition++) {
                try {
                    client.send("partial", partition, "m".getBytes(StandardCharsets.UTF_8));
                } catch (IOException e) {
                    failures.add(partition + ": " + e.getMessage());
                }
            }
            assertEquals(List.of(), failures, "produces that failed");
        }
    }

    private static void createPartitions(ZooKeeper zk, int from, int to) throws Exception {
        for (int id = from; id < to; id++) {
            createNode(zk, "/topics/partial/partitions/" + id, "1;");
        }
    }

    private static void createNode(ZooKeeper zk, String path) throws Exception {
        createNode(zk, path, "");
    }

    private static void createNode(ZooKeeper zk, String path, String data) throws Exception {
        zk.create(path, data.getBytes(StandardCharsets.UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }
}
