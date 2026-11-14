package com.simplekafka;

import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

class ConnectionHandlingTest {

    @Test
    void idleConnectionsDoNotBlockOtherClients() throws Exception {
        try (TestCluster cluster = TestCluster.start(1)) {
            List<SocketChannel> idle = new ArrayList<>();
            try {
                for (int i = 0; i < 12; i++) {
                    idle.add(SocketChannel.open(new InetSocketAddress("localhost", cluster.port(1))));
                }
                assertTimeoutPreemptively(Duration.ofSeconds(3), () -> cluster.client(1));
            } finally {
                for (SocketChannel channel : idle) {
                    channel.close();
                }
            }
        }
    }

    @Test
    void sequentialRequestsAreNotPacedByTheAcceptLoop() throws Exception {
        try (TestCluster cluster = TestCluster.start(1)) {
            assertTimeoutPreemptively(Duration.ofSeconds(1), () -> {
                for (int i = 0; i < 20; i++) {
                    cluster.client(1);
                }
            });
        }
    }
}
