package com.simplekafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.CompletableFuture;

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
}
