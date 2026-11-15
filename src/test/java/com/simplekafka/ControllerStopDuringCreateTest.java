package com.simplekafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.jupiter.api.Test;

import com.simplekafka.broker.Protocol;

class ControllerStopDuringCreateTest {

    // Every broker treats an existing /topics/<name> as a complete topic, so a controller that stops partway
    // through a create must leave either the whole topic in ZooKeeper or none of it.
    @Test
    void controllerStoppingMidCreateLeavesTheWholeTopicOrNone() throws Exception {
        try (TestCluster cluster = TestCluster.start(3);
                ZooKeeper zk = cluster.openZooKeeper()) {
            assertEquals("1", new String(zk.getData("/controller", false, null)), "controller");
            CompletableFuture<Void> created = CompletableFuture.runAsync(() -> {
                try {
                    cluster.request(1, Protocol.encodeCreateTopicRequest("wide", 400, (short) 2));
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
            TestCluster.await("partition 20 of wide in ZooKeeper", () -> {
                try {
                    return zk.exists("/topics/wide/partitions/20", false) != null;
                } catch (Exception e) {
                    return false;
                }
            });
            cluster.stopBroker(1);
            created.handle((ignored, e) -> null).get();
            TestCluster.await("a new controller", () -> {
                try {
                    return !new String(zk.getData("/controller", false, null)).equals("1");
                } catch (Exception e) {
                    return false;
                }
            });

            if (zk.exists("/topics/wide", false) == null) {
                assertTrue(cluster.client(2).createTopic("wide", 400, (short) 2), "retried create");
            }
            assertEquals(400, zk.getChildren("/topics/wide/partitions", false).size(),
                    "partition znodes of wide in ZooKeeper");
        }
    }

    // A controller that commits a topic and stops before it tells any broker leaves a topic no live broker has
    // loaded, all of it led by the stopped broker. The test writes that topic to ZooKeeper itself.
    @Test
    void nextControllerMovesPartitionsOfATopicNoBrokerLoaded() throws Exception {
        try (TestCluster cluster = TestCluster.start(3);
                ZooKeeper zk = cluster.openZooKeeper()) {
            assertEquals("1", new String(zk.getData("/controller", false, null)), "controller");
            List<Op> topic = new ArrayList<>();
            topic.add(Op.create("/topics/unseen", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
            topic.add(Op.create("/topics/unseen/partitions", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT));
            for (int partition = 0; partition < 6; partition++) {
                topic.add(Op.create("/topics/unseen/partitions/" + partition,
                        ("1;" + (2 + partition % 2) + ",").getBytes(StandardCharsets.UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT));
            }
            zk.multi(topic);
            cluster.stopBroker(1);

            assertEquals(List.of(), cluster.awaitAgreementWithZooKeeper(zk, "unseen", 2, 3),
                    "brokers whose metadata for unseen differs from ZooKeeper");
        }
    }
}
