package com.simplekafka;

import java.io.File;
import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

import com.simplekafka.broker.Protocol;
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
        zkFactory = ServerCnxnFactory.createFactory(new InetSocketAddress(0), 100);
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
        // freePort() closes its socket before the broker binds, so another process can take the port in between.
        for (int attempt = 1; ; attempt++) {
            int port = freePort();
            SimpleKafkaBroker broker = new SimpleKafkaBroker(id, "localhost", port, zkFactory.getLocalPort());
            try {
                broker.start();
            } catch (BindException e) {
                if (attempt == 5) {
                    throw e;
                }
                continue;
            }
            brokers.add(broker);
            brokerPorts.add(port);
            return;
        }
    }

    public int zkPort() {
        return zkFactory.getLocalPort();
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
        return Protocol.call("localhost", port(brokerId), request);
    }

    /**
     * The messages in one broker's log for a partition, read from its segment files, since a broker answers a
     * fetch only for partitions it leads. Stops at a record the broker is still writing.
     */
    public static List<byte[]> messagesOnDisk(int brokerId, String topic, int partition) throws IOException {
        List<Path> segments;
        try (Stream<Path> files = Files.list(Path.of("data", String.valueOf(brokerId), topic, String.valueOf(partition)))) {
            segments = files.filter(p -> p.toString().endsWith(".log")).sorted().collect(Collectors.toList());
        }
        List<byte[]> messages = new ArrayList<>();
        for (Path segment : segments) {
            ByteBuffer log = ByteBuffer.wrap(Files.readAllBytes(segment));
            while (log.remaining() >= 4) {
                int size = log.getInt();
                if (size < 0 || size > log.remaining()) {
                    return messages;
                }
                byte[] message = new byte[size];
                log.get(message);
                messages.add(message);
            }
        }
        return messages;
    }

    /**
     * Each way the given brokers' metadata for a topic differs from ZooKeeper's partition nodes, plus the
     * partitions ZooKeeper gives to a broker with no node under /brokers. Empty when everything agrees.
     */
    public List<String> disagreementsWithZooKeeper(ZooKeeper zk, String topic, int... brokerIds) throws Exception {
        Map<Integer, Integer> zkLeaders = new HashMap<>();
        for (String id : zk.getChildren("/topics/" + topic + "/partitions", false)) {
            String data = new String(zk.getData("/topics/" + topic + "/partitions/" + id, false, null),
                    StandardCharsets.UTF_8);
            zkLeaders.put(Integer.parseInt(id), Integer.parseInt(data.split(";")[0]));
        }
        List<String> registered = zk.getChildren("/brokers", false);
        List<String> found = new ArrayList<>();
        long orphaned = zkLeaders.values().stream().filter(leader -> !registered.contains(String.valueOf(leader))).count();
        if (orphaned > 0) {
            found.add("ZooKeeper: " + orphaned + " partitions led by an unregistered broker");
        }
        for (int brokerId : brokerIds) {
            SimpleKafkaClient.TopicMetadata metadata;
            try {
                metadata = client(brokerId).getTopicMetadata(topic);
            } catch (IOException e) {
                found.add("broker " + brokerId + ": unreachable");
                continue;
            }
            Map<Integer, Integer> leaders = new HashMap<>();
            if (metadata != null) {
                metadata.getPartitions().forEach(p -> leaders.put(p.getId(), p.getLeader()));
            }
            long missing = zkLeaders.keySet().stream().filter(id -> !leaders.containsKey(id)).count();
            long differ = zkLeaders.entrySet().stream()
                    .filter(e -> leaders.containsKey(e.getKey()) && !leaders.get(e.getKey()).equals(e.getValue()))
                    .count();
            if (missing > 0) {
                found.add("broker " + brokerId + ": " + missing + " of " + zkLeaders.size() + " partitions missing");
            }
            if (differ > 0) {
                found.add("broker " + brokerId + ": " + differ + " leaders differ from ZooKeeper");
            }
        }
        return found;
    }

    /** Polls disagreementsWithZooKeeper until it is empty or 20 s pass, and returns its last result. */
    public List<String> awaitAgreementWithZooKeeper(ZooKeeper zk, String topic, int... brokerIds) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        List<String> found = disagreementsWithZooKeeper(zk, topic, brokerIds);
        while (!found.isEmpty() && System.nanoTime() < deadline) {
            Thread.sleep(100);
            found = disagreementsWithZooKeeper(zk, topic, brokerIds);
        }
        return found;
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
