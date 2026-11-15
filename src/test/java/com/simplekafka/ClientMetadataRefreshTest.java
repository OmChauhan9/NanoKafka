package com.simplekafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import com.simplekafka.client.SimpleKafkaClient;

class ClientMetadataRefreshTest {

    // Every refresh opens a new connection, so the refresh and fetch counts stay low enough
    // not to run the machine out of ephemeral ports.
    private static final int REFRESHES = 200;
    private static final int MAX_FETCHES = 100;

    @Test
    void anExistingTopicStaysVisibleToOtherThreadsWhileMetadataRefreshes() throws Exception {
        try (TestCluster cluster = TestCluster.start(1)) {
            SimpleKafkaClient client = cluster.client(1);
            assertTrue(client.createTopic("shared", 1, (short) 1));
            client.send("shared", 0, new byte[] { 1 });

            AtomicReference<Throwable> refreshFailure = new AtomicReference<>();
            Thread refresher = new Thread(() -> {
                try {
                    for (int i = 0; i < REFRESHES; i++) {
                        client.refreshMetadata();
                    }
                } catch (Throwable e) {
                    refreshFailure.set(e);
                }
            });
            refresher.start();

            long lookups = 0;
            int missing = 0;
            int fetches = 0;
            Throwable fetchFailure = null;
            while (refresher.isAlive()) {
                lookups++;
                if (client.getTopicMetadata("shared") == null) {
                    missing++;
                }
                if (fetches < MAX_FETCHES && lookups % 1000 == 0) {
                    fetches++;
                    try {
                        client.fetch("shared", 0, 0, 1024);
                    } catch (Throwable e) {
                        if (fetchFailure == null) {
                            fetchFailure = e;
                        }
                    }
                }
            }
            refresher.join();

            assertEquals(0, missing, "getTopicMetadata returned null for an existing topic in " + lookups + " lookups");
            assertNull(fetchFailure, "fetch of an existing topic failed during a refresh");
            assertNull(refreshFailure.get(), "refreshMetadata failed");
        }
    }
}
