package com.simplekafka;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

import com.simplekafka.broker.SimpleKafkaBroker;
import com.simplekafka.client.SimpleKafkaClient;

/**
 * An embedded ZooKeeper plus N brokers on free localhost ports, for tests.
 * Brokers write to ./data, so tests run from target/test-cwd and this class
 * clears ./data on start.
 */
public final class TestCluster implements AutoCloseable {
    private final Path zkDir;
    private final ZooKeeperServer zkServer;
    private final ServerCnxnFactory zkFactory;
    private final List<SimpleKafkaBroker> brokers = new ArrayList<>();
    private final List<Integer> brokerPorts = new ArrayList<>();

    private TestCluster() throws IOException, InterruptedException {
        deleteRecursively(Path.of("data"));
        zkDir = Files.createTempDirectory("nanokafka-zk");
        zkServer = new ZooKeeperServer(zkDir.toFile(), zkDir.toFile(), 2000);
        zkFactory = ServerCnxnFactory.createFactory(new InetSocketAddress("localhost", 0), 100);
        zkFactory.startup(zkServer);
    }

    public static TestCluster start(int brokerCount) throws Exception {
        TestCluster cluster = new TestCluster();
        try {
            for (int id = 1; id <= brokerCount; id++) {
                cluster.addBroker(id);
            }
            cluster.awaitAllBrokersSee(brokerCount);
            return cluster;
        } catch (Exception e) {
            cluster.close();
            throw e;
        }
    }

    public void addBroker(int id) throws IOException {
        int port = freePort();
        SimpleKafkaBroker broker = new SimpleKafkaBroker(id, "localhost", port, zkFactory.getLocalPort());
        broker.start();
        brokers.add(broker);
        brokerPorts.add(port);
    }

    public void restartBroker(int brokerId) throws IOException {
        SimpleKafkaBroker broker = new SimpleKafkaBroker(brokerId, "localhost", port(brokerId), zkFactory.getLocalPort());
        broker.start();
        brokers.set(brokerId - 1, broker);
    }

    public int port(int brokerId) {
        return brokerPorts.get(brokerId - 1);
    }

    /** A raw ZooKeeper session, separate from every broker's, with the server's minimum session timeout. */
    public ZooKeeper openZooKeeper() throws IOException, InterruptedException {
        CountDownLatch connected = new CountDownLatch(1);
        ZooKeeper zk = new ZooKeeper("localhost:" + zkFactory.getLocalPort(), 4000, event -> {
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                connected.countDown();
            }
        });
        if (!connected.await(10, TimeUnit.SECONDS)) {
            zk.close();
            throw new IOException("could not connect to the embedded ZooKeeper");
        }
        return zk;
    }

    public SimpleKafkaClient client(int brokerId) throws IOException {
        SimpleKafkaClient client = new SimpleKafkaClient("localhost", port(brokerId));
        client.initialize();
        return client;
    }

    /** Sends one raw request to one broker, bypassing the client's leader routing. */
    public ByteBuffer request(int brokerId, ByteBuffer request) throws IOException {
        try (SocketChannel channel = SocketChannel.open(new InetSocketAddress("localhost", port(brokerId)))) {
            channel.write(request);
            ByteBuffer response = ByteBuffer.allocate(64 * 1024);
            channel.read(response);
            response.flip();
            return response;
        }
    }

    public void awaitTopicEverywhere(String topic) {
        await("topic " + topic + " on every broker", () -> {
            for (int id = 1; id <= brokers.size(); id++) {
                try {
                    if (client(id).getTopicMetadata(topic) == null) {
                        return false;
                    }
                } catch (IOException e) {
                    return false;
                }
            }
            return true;
        });
    }

    private void awaitAllBrokersSee(int count) {
        await(count + " brokers in every broker's metadata", () -> {
            for (int id = 1; id <= brokers.size(); id++) {
                try {
                    if (client(id).getBrokers().size() != count) {
                        return false;
                    }
                } catch (IOException e) {
                    return false;
                }
            }
            return true;
        });
    }

    public static void await(String what, BooleanSupplier condition) {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("interrupted waiting for " + what, e);
            }
        }
        throw new AssertionError("timed out waiting for " + what);
    }

    public void stopBroker(int brokerId) {
        brokers.get(brokerId - 1).stop();
    }

    @Override
    public void close() {
        for (SimpleKafkaBroker broker : brokers) {
            broker.stop();
        }
        zkFactory.shutdown();
        zkServer.shutdown();
        deleteRecursively(zkDir);
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    static void deleteRecursively(Path root) {
        if (!Files.exists(root)) {
            return;
        }
        try (Stream<Path> paths = Files.walk(root)) {
            paths.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
        } catch (IOException e) {
            throw new IllegalStateException("could not delete " + root, e);
        }
    }
}
