package com.simplekafka;

import static org.junit.jupiter.api.Assertions.assertFalse;

import org.junit.jupiter.api.Test;

class CreateTopicTest {

    @Test
    void createReportsFailureWhenZooKeeperRejectsTheTopic() throws Exception {
        try (TestCluster cluster = TestCluster.start(1)) {
            assertFalse(cluster.client(1).createTopic("a/b", 1, (short) 1),
                    "createTopic must report the ZooKeeper failure");
        }
    }
}
