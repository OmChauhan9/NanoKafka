package com.simplekafka;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

/**
 * A broker registered in ZooKeeper that accepts connections and never answers, so a create that notifies it
 * waits the controller's full 5 s for its acknowledgement.
 */
final class SilentBroker implements AutoCloseable {
    private final ServerSocket server = new ServerSocket(0);
    private final List<Socket> accepted = Collections.synchronizedList(new ArrayList<>());

    SilentBroker() throws IOException {
        Thread acceptor = new Thread(() -> {
            while (!server.isClosed()) {
                try {
                    accepted.add(server.accept());
                } catch (IOException e) {
                    return;
                }
            }
        });
        acceptor.setDaemon(true);
        acceptor.start();
    }

    /** Registers this broker under the id, in the given session, the way a real broker does. */
    SilentBroker register(ZooKeeper session, long brokerId) throws Exception {
        session.create("/brokers/" + brokerId, ("localhost:" + server.getLocalPort()).getBytes(StandardCharsets.UTF_8),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        return this;
    }

    @Override
    public void close() throws IOException {
        server.close();
        synchronized (accepted) {
            for (Socket socket : accepted) {
                socket.close();
            }
        }
    }
}
