package com.simplekafka;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.Set;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.jupiter.api.Test;

class StaleBrokerSessionTest {

    @Test
    void restartedBrokerRegistersOnceItsDeadSessionIsGone() throws Exception {
        try (TestCluster cluster = TestCluster.start(0);
                ZooKeeper zk = cluster.openZooKeeper()) {
            zk.create("/brokers", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            ZooKeeper deadSession = cluster.openZooKeeper();
            deadSession.create("/brokers/1", "localhost:1".getBytes(StandardCharsets.UTF_8),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

            cluster.addBroker(1);
            deadSession.close();

            TestCluster.await("broker 1 to re-register in ZooKeeper", () -> {
                try {
                    return zk.exists("/brokers/1", false) != null;
                } catch (KeeperException | InterruptedException e) {
                    return false;
                }
            });
            assertEquals(Set.of(1), cluster.client(1).getBrokers().keySet(), "broker 1 lists itself");
        }
    }
}
