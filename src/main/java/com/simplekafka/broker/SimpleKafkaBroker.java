package com.simplekafka.broker;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ProtocolException;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.zookeeper.KeeperException;

public class SimpleKafkaBroker {
    private static final Logger LOGGER = Logger.getLogger(SimpleKafkaBroker.class.getName());
    private static final String DATA_DIR = "data";

    private final int brokerId;
    private final String brokerHost;
    private final int brokerPort;
    private final Map<String, List<Partition>> topics;
    private final ExecutorService executor;
    private final ServerSocketChannel serverChannel;
    private final AtomicBoolean isRunning;
    private final AtomicBoolean isController;
    private final Map<Integer, BrokerInfo> clusterMetadata;
    private final ZookeeperClient zkClient;
    private final Set<SocketChannel> clientChannels = ConcurrentHashMap.newKeySet();
    // Broker changes, elections, rebalances and topic creation run here one at a time, so each sees the effects
    // of the last, and none runs on ZooKeeper's event thread.
    private final ScheduledExecutorService clusterEvents = Executors.newSingleThreadScheduledExecutor();
    // The brokers this controller has sent every topic since it took over. Used only on clusterEvents.
    private final Set<Integer> syncedBrokers = new HashSet<>();

    public SimpleKafkaBroker(int brokerId, String host, int port, int zkPort) throws IOException {
        this.brokerId = brokerId;
        this.brokerHost = host;
        this.brokerPort = port;
        this.topics = new ConcurrentHashMap<>();
        this.executor = Executors.newCachedThreadPool();
        this.serverChannel = ServerSocketChannel.open();
        this.isRunning = new AtomicBoolean(false);
        this.isController = new AtomicBoolean(false);
        this.clusterMetadata = new ConcurrentHashMap<>();

        // Initialize data directory
        File dataDir = new File(DATA_DIR + File.separator + brokerId);
        if (!dataDir.exists()) {
            dataDir.mkdirs();
        }

        // Initialize ZooKeeper client
        this.zkClient = new ZookeeperClient("localhost", zkPort);
    }

    /**
     * Tell a broker to load the topic, or reload its partition assignments, from ZooKeeper
     */
    private Future<Boolean> notifyBrokerOfTopic(int brokerId, String topic) {
        BrokerInfo broker = clusterMetadata.get(brokerId);
        if (broker == null)
            return CompletableFuture.completedFuture(false);

        return executor.submit(() -> {
            try {
                return Protocol.call(broker.getHost(), broker.getPort(), Protocol.encodeTopicNotification(topic)).get() == 0;
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Failed to notify broker " + brokerId + " about topic " + topic, e);
                return false;
            }
        });
    }

    /**
     * Handle topic notification from controller
     */
    private void handleTopicNotification(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        String topic = Protocol.readString(buffer);

        LOGGER.info("Received topic notification for: " + topic);

        // Load topic metadata from ZooKeeper
        try {
            loadTopic(topic);

            // Send acknowledgment
            ByteBuffer response = ByteBuffer.allocate(1);
            response.put((byte) 0); // Acknowledgment
            response.flip();
            Protocol.writeFrame(clientChannel, response);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to load topic: " + topic, e);

            // Send error response
            ByteBuffer response = ByteBuffer.allocate(1);
            response.put((byte) 1); // Error
            response.flip();
            Protocol.writeFrame(clientChannel, response);
        }
    }

    /**
     * Load topic metadata from ZooKeeper, or refresh a loaded topic: update the leader and followers of its
     * partitions and add any partition ZooKeeper has that this broker does not.
     */
    private synchronized void loadTopic(String topic) throws Exception {
        String topicPath = "/topics/" + topic;
        if (!zkClient.exists(topicPath)) {
            throw new Exception("Topic does not exist in ZooKeeper: " + topic);
        }

        String topicDir = DATA_DIR + File.separator + brokerId + File.separator + topic;
        new File(topicDir).mkdirs();

        List<Partition> loaded = topics.get(topic);
        List<String> partitionIds = zkClient.getChildren(topicPath + "/partitions");
        List<Partition> partitions = new ArrayList<>();

        for (String partitionId : partitionIds) {
            int id = Integer.parseInt(partitionId);
            String partitionPath = topicPath + "/partitions/" + partitionId;
            String partitionData = zkClient.getData(partitionPath);

            String[] parts = partitionData.split(";");
            int leader = Integer.parseInt(parts[0]);

            List<Integer> followers = new ArrayList<>();
            if (parts.length > 1 && !parts[1].isEmpty()) {
                String[] followerIds = parts[1].split(",");
                for (String followerId : followerIds) {
                    if (!followerId.isEmpty()) {
                        followers.add(Integer.parseInt(followerId));
                    }
                }
            }

            Partition existing = loaded == null ? null
                    : loaded.stream().filter(p -> p.getId() == id).findFirst().orElse(null);
            if (existing != null) {
                existing.setLeader(leader);
                existing.setFollowers(followers);
                partitions.add(existing);
                continue;
            }

            String partitionDir = topicDir + File.separator + id;
            new File(partitionDir).mkdirs();

            Partition partition = new Partition(id, leader, followers, partitionDir);
            partitions.add(partition);

            LOGGER.info("Loaded partition " + id + " for topic " + topic +
                    ", leader: " + leader + ", followers: " + followers);
        }

        // A new list, not an in-place add, because request threads iterate the published one.
        topics.put(topic, partitions);
        LOGGER.info("Successfully loaded topic: " + topic + " with " + partitions.size() + " partitions");
    }

    /**
     * Load all topics from ZooKeeper
     */
    public void loadTopics() {
        try {
            List<String> topicNames = zkClient.getChildren("/topics");

            for (String topic : topicNames) {
                try {
                    loadTopic(topic);
                } catch (Exception e) {
                    LOGGER.log(Level.SEVERE, "Failed to load topic: " + topic, e);
                }
            }

            LOGGER.info("Loaded " + topics.size() + " topics");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to load topics", e);
        }
    }

    /**
     * Main entry point for running a broker
     */
    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Usage: SimpleKafkaBroker <brokerId> <host> <port> [zkPort]");
            System.exit(1);
        }

        try {
            int brokerId = Integer.parseInt(args[0]);
            String host = args[1];
            int port = Integer.parseInt(args[2]);
            int zkPort = args.length > 3 ? Integer.parseInt(args[3]) : 2181;

            SimpleKafkaBroker broker = new SimpleKafkaBroker(brokerId, host, port, zkPort);
            broker.start();

            // Add shutdown hook
            Runtime.getRuntime().addShutdownHook(new Thread(broker::stop));

            System.out.println("SimpleKafka broker started. Press Ctrl+C to stop.");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to start broker", e);
        }
    }

    /**
     * Start the broker server
     */
    public void start() throws IOException {
        if (isRunning.compareAndSet(false, true)) {
            // Bind to socket
            serverChannel.socket().bind(new InetSocketAddress(brokerHost, brokerPort));

            LOGGER.info("SimpleKafka broker started on " + brokerHost + ":" + brokerPort);

            // Register with ZooKeeper
            registerWithZookeeper();

            // A create sent as soon as start returns needs the election settled.
            try {
                clusterEvents.submit(this::electController).get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
                LOGGER.log(Level.SEVERE, "Controller election failed", e);
            }

            // Accept client connections
            executor.submit(this::acceptConnections);
        }
    }

    /**
     * Stop the broker server
     */
    public void stop() {
        if (isRunning.compareAndSet(true, false)) {
            try {
                LOGGER.info("Stopping SimpleKafka broker...");

                // Close server socket
                serverChannel.close();
                for (SocketChannel clientChannel : clientChannels) {
                    closeClient(clientChannel);
                }

                // Before the partitions close, because a rebalance loads topics.
                clusterEvents.shutdownNow();
                clusterEvents.awaitTermination(5, TimeUnit.SECONDS);

                // Close all topic partitions
                for (List<Partition> partitions : topics.values()) {
                    for (Partition partition : partitions) {
                        partition.close();
                    }
                }

                // Shut down executor
                executor.shutdownNow();
                executor.awaitTermination(5, TimeUnit.SECONDS);

                // Close ZooKeeper connection
                zkClient.close();

                LOGGER.info("SimpleKafka broker stopped");
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Error stopping broker", e);
            }
        }
    }

    /**
     * Register this broker with ZooKeeper
     */
    private void registerWithZookeeper() {
        try {
            zkClient.connect();

            loadTopics();

            createBrokerNode();

            // Add broker info to local metadata
            BrokerInfo selfInfo = new BrokerInfo(brokerId, brokerHost, brokerPort);
            clusterMetadata.put(brokerId, selfInfo);

            // Watch for other brokers
            zkClient.watchChildren("/brokers", brokerIds -> queueClusterEvent(this::onBrokersChanged, 0));

            LOGGER.info("Registered with ZooKeeper at " + zkClient.getConnectString());
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to register with ZooKeeper", e);
        }
    }

    /**
     * Create this broker's ephemeral node. Returns false while another session still holds it.
     */
    private boolean createBrokerNode() {
        try {
            return zkClient.createEphemeralNode("/brokers/" + brokerId, brokerHost + ":" + brokerPort);
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Failed to create broker node", e);
            return false;
        }
    }

    /**
     * Run an event on clusterEvents after the given delay. A stopped broker drops it.
     */
    private void queueClusterEvent(Runnable event, long delayMillis) {
        try {
            clusterEvents.schedule(() -> {
                try {
                    event.run();
                } catch (RuntimeException e) {
                    LOGGER.log(Level.SEVERE, "Cluster event failed", e);
                }
            }, delayMillis, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            LOGGER.log(Level.FINE, "Dropped a cluster event after stop", e);
        }
    }

    /**
     * Handle changes in the broker list from ZooKeeper
     */
    private void onBrokersChanged() {
        List<String> brokerIds;
        try {
            brokerIds = refreshBrokers();
        } catch (KeeperException | InterruptedException e) {
            LOGGER.log(Level.WARNING, "Failed to read the broker list", e);
            return;
        }

        // A node left by this broker's previous, dead session blocks registration until it expires.
        boolean registered = brokerIds.contains(String.valueOf(brokerId)) || createBrokerNode();

        // Re-elect controller if needed
        if (!registered && isController.get()) {
            isController.set(false);
            LOGGER.info("This broker is no longer in the cluster, giving up controller status");
        } else if (isController.get()) {
            // As controller, rebalance partitions due to cluster changes
            rebalancePartitions();
        } else {
            // Re-attempt controller election
            electController();
        }
    }

    /**
     * Read the registered brokers from ZooKeeper into clusterMetadata and return their ids.
     */
    private List<String> refreshBrokers() throws KeeperException, InterruptedException {
        List<String> brokerIds = zkClient.getChildren("/brokers");
        LOGGER.info("Current brokers: " + brokerIds);

        // Update cluster metadata
        for (String id : brokerIds) {
            try {
                int brokerId = Integer.parseInt(id);
                if (!clusterMetadata.containsKey(brokerId)) {
                    String brokerData = zkClient.getData("/brokers/" + id);
                    String[] hostPort = brokerData.split(":");
                    BrokerInfo info = new BrokerInfo(
                            brokerId,
                            hostPort[0],
                            Integer.parseInt(hostPort[1]));
                    clusterMetadata.put(brokerId, info);
                    LOGGER.info("Added broker: " + info);
                }
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Failed to process broker info", e);
            }
        }

        // Remove brokers that have disappeared
        List<Integer> toRemove = new ArrayList<>();
        for (Integer brokerId : clusterMetadata.keySet()) {
            if (brokerId != this.brokerId && !brokerIds.contains(String.valueOf(brokerId))) {
                toRemove.add(brokerId);
            }
        }

        for (Integer brokerId : toRemove) {
            clusterMetadata.remove(brokerId);
            LOGGER.info("Removed broker: " + brokerId);
        }
        // Once back, a broker that left may hold state it read before changes nobody told it about.
        syncedBrokers.retainAll(clusterMetadata.keySet());
        return brokerIds;
    }

    /**
     * Participate in controller election
     */
    private void electController() {
        if (!isRunning.get()) {
            return;
        }
        try {
            String controllerPath = "/controller";

            // First, make sure the node doesn't already exist (or is empty)
            boolean nodeExists = zkClient.exists(controllerPath);
            if (nodeExists) {
                String existingData = zkClient.getData(controllerPath);
                if (existingData == null || existingData.trim().isEmpty()) {
                    // Node exists but has empty data, try to delete it
                    zkClient.deleteNode(controllerPath);
                    nodeExists = false;
                    LOGGER.info("Deleted empty controller node");
                }
            }

            // Now try to create the node
            boolean becameController = false;
            if (!nodeExists) {
                becameController = zkClient.createEphemeralNode(controllerPath, String.valueOf(brokerId));
            }

            if (becameController) {
                isController.set(true);
                syncedBrokers.clear();
                LOGGER.info("This broker is now the active controller");

                // As controller, ensure all topics are properly replicated
                rebalancePartitions();
            } else {
                // Double-check the data
                String controllerId = zkClient.getData(controllerPath);
                if (controllerId == null || controllerId.trim().isEmpty()) {
                    LOGGER.warning("Controller node exists but has no data. This is unexpected.");
                    queueClusterEvent(this::electController, 1000);
                    return;
                }

                LOGGER.info("Current controller is broker " + controllerId);

                // Watch controller node for changes
                zkClient.watchNode(controllerPath, this::onControllerChange);
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Controller election failed", e);
            queueClusterEvent(this::electController, 2000);
        }
    }

    /**
     * Handle controller change notification
     */
    private void onControllerChange() {
        LOGGER.info("Controller changed, initiating new election");
        queueClusterEvent(this::electController, 0);
    }

    /**
     * Move partitions led by a broker that left to a live replica, and tell the other brokers. A broker this
     * controller has not synced since it took over gets every topic, because it may have read ZooKeeper before
     * the latest create or reassignment and no notice since then went to it.
     */
    private void rebalancePartitions() {
        if (!isController.get()) {
            return;
        }

        LOGGER.info("Rebalancing partitions across cluster");
        try {
            refreshBrokers();
        } catch (KeeperException | InterruptedException e) {
            LOGGER.log(Level.WARNING, "Failed to read the broker list", e);
            return;
        }
        // The previous controller may have created topics this broker never loaded.
        loadTopics();

        Set<String> reassignedTopics = new HashSet<>();
        for (Map.Entry<String, List<Partition>> entry : topics.entrySet()) {
            String topic = entry.getKey();
            boolean reassigned = false;

            for (Partition partition : entry.getValue()) {
                int oldLeader = partition.getLeader();
                if (oldLeader != -1 && clusterMetadata.containsKey(oldLeader)) {
                    continue;
                }

                // Only replicas hold the partition's log, so the leader moves within the replica set. A dead
                // replica stays listed as a follower so it can lead again when it returns.
                List<Integer> replicas = new ArrayList<>();
                if (oldLeader != -1) {
                    replicas.add(oldLeader);
                }
                replicas.addAll(partition.getFollowers());
                int newLeader = replicas.stream().filter(clusterMetadata::containsKey).findFirst().orElse(-1);
                if (newLeader == oldLeader) {
                    continue;
                }
                replicas.remove(Integer.valueOf(newLeader));
                partition.setLeader(newLeader);
                partition.setFollowers(replicas);
                updatePartitionMetadata(topic, partition);
                reassigned = true;

                LOGGER.info("Reassigned partition " + partition.getId() +
                        " of topic " + topic +
                        " to leader " + newLeader +
                        " with followers " + replicas);
            }

            if (reassigned) {
                reassignedTopics.add(topic);
            }
        }

        for (int brokerId : clusterMetadata.keySet()) {
            if (brokerId == this.brokerId) {
                continue;
            }
            Collection<String> stale = syncedBrokers.add(brokerId) ? topics.keySet() : reassignedTopics;
            for (String topic : stale) {
                notifyBrokerOfTopic(brokerId, topic);
            }
        }
    }

    /**
     * Update partition metadata in ZooKeeper
     */
    private void updatePartitionMetadata(String topic, Partition partition) {
        try {
            String path = "/topics/" + topic + "/partitions/" + partition.getId();
            String data = partition.getLeader() + ";";
            for (int follower : partition.getFollowers()) {
                data += follower + ",";
            }

            if (zkClient.exists(path)) {
                zkClient.setData(path, data);
            } else {
                zkClient.createPersistentNode(path, data);
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to update partition metadata", e);
        }
    }

    /**
     * Accept client connections
     */
    private void acceptConnections() {
        while (isRunning.get()) {
            try {
                SocketChannel clientChannel = serverChannel.accept();
                if (clientChannel != null) {
                    LOGGER.info("Accepted connection from " + clientChannel.getRemoteAddress());
                    clientChannels.add(clientChannel);
                    // stop() clears isRunning before it closes clientChannels, so a channel accepted during
                    // shutdown is closed either here or by stop().
                    if (!isRunning.get()) {
                        closeClient(clientChannel);
                        continue;
                    }
                    try {
                        executor.submit(() -> handleClient(clientChannel));
                    } catch (RejectedExecutionException e) {
                        closeClient(clientChannel);
                    }
                }
            } catch (Exception e) {
                if (isRunning.get()) {
                    LOGGER.log(Level.SEVERE, "Error accepting connection", e);
                }
            }
        }
    }

    /**
     * Handle client connection
     */
    private void closeClient(SocketChannel clientChannel) {
        clientChannels.remove(clientChannel);
        try {
            clientChannel.close();
        } catch (IOException e) {
            LOGGER.log(Level.FINE, "Error closing client channel", e);
        }
    }

    private void handleClient(SocketChannel clientChannel) {
        try (clientChannel) {
            while (true) {
                ByteBuffer request;
                try {
                    request = Protocol.readFrame(clientChannel);
                } catch (ProtocolException e) {
                    Protocol.sendErrorResponse(clientChannel, e.getMessage());
                    return;
                }
                if (request == null) {
                    return;
                }
                processClientMessage(clientChannel, request);
            }
        } catch (Exception e) {
            if (isRunning.get()) {
                LOGGER.log(Level.SEVERE, "Error handling client", e);
            }
        } finally {
            clientChannels.remove(clientChannel);
        }
    }

    /**
     * Process client message based on SimpleKafka wire protocol
     */
    private void processClientMessage(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        byte messageType = buffer.get();

        switch (messageType) {
            case Protocol.PRODUCE:
                handleProduceRequest(clientChannel, buffer, true);
                break;
            case Protocol.FORWARDED_PRODUCE:
                handleProduceRequest(clientChannel, buffer, false);
                break;
            case Protocol.FETCH:
                handleFetchRequest(clientChannel, buffer);
                break;
            case Protocol.METADATA:
                handleMetadataRequest(clientChannel, buffer);
                break;
            case Protocol.CREATE_TOPIC:
                handleCreateTopicRequest(clientChannel, buffer);
                break;
            case Protocol.REPLICATE:
                handleReplicateRequest(clientChannel, buffer);
                break;
            case Protocol.TOPIC_NOTIFICATION:
                handleTopicNotification(clientChannel, buffer);
                break;
            default:
                LOGGER.warning("Unknown message type: " + messageType);
                Protocol.sendErrorResponse(clientChannel, "Unknown message type");
        }
    }

    /**
     * Handle produce request from client
     */
    private void handleProduceRequest(SocketChannel clientChannel, ByteBuffer buffer, boolean mayForward)
            throws IOException {
        String topic = Protocol.readString(buffer);

        int partition = buffer.getInt();
        int messageSize = buffer.getInt();
        byte[] message = new byte[messageSize];
        buffer.get(message);

        LOGGER.info("Produce request for topic: " + topic + ", partition: " + partition);

        if (messageSize > Protocol.maxMessageBytes(topic)) {
            Protocol.sendErrorResponse(clientChannel, "Message of " + messageSize
                    + " bytes exceeds the largest of " + Protocol.maxMessageBytes(topic) + " bytes this topic accepts");
            return;
        }

        // Check if topic exists
        if (!topics.containsKey(topic)) {
            Protocol.sendErrorResponse(clientChannel, "Topic does not exist");
            return;
        }

        // Find the partition
        List<Partition> partitions = topics.get(topic);
        Partition targetPartition = null;

        for (Partition p : partitions) {
            if (p.getId() == partition) {
                targetPartition = p;
                break;
            }
        }

        if (targetPartition == null) {
            Protocol.sendErrorResponse(clientChannel, "Partition does not exist");
            return;
        }

        // Check if this broker is the leader for the partition
        if (targetPartition.getLeader() != brokerId) {
            // Two brokers that each name the other as leader would pass one produce back and forth forever.
            if (!mayForward) {
                Protocol.sendErrorResponse(clientChannel, "Not the leader for partition " + partition);
                return;
            }
            forwardProduceToLeader(clientChannel, topic, partition, message, targetPartition.getLeader());
            return;
        }

        // Append message to log
        long offset = targetPartition.append(message);

        if (offset >= 0) {
            replicateToFollowers(topic, targetPartition, message, offset);
        }

        // Send acknowledgment to client
        ByteBuffer response = ByteBuffer.allocate(10);
        response.put(Protocol.PRODUCE_RESPONSE);
        response.putLong(offset);
        response.put((byte) (offset > -1 ? 0 : 1)); // 0 = success, 1 = error
        response.flip();
        Protocol.writeFrame(clientChannel, response);
    }

    /**
     * Forward produce request to leader broker
     */
    private void forwardProduceToLeader(SocketChannel clientChannel, String topic, int partition,
            byte[] message, int leaderId) throws IOException {
        BrokerInfo leader = clusterMetadata.get(leaderId);
        if (leader == null) {
            Protocol.sendErrorResponse(clientChannel, "Leader broker not available");
            return;
        }

        ByteBuffer response;
        try {
            response = Protocol.call(leader.getHost(), leader.getPort(),
                    Protocol.encodeForwardedProduceRequest(topic, partition, message));
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to forward produce request to leader", e);
            Protocol.sendErrorResponse(clientChannel, "Failed to forward to leader");
            return;
        }
        Protocol.writeFrame(clientChannel, response);
    }

    /**
     * Replicate message to follower brokers
     */
    private void replicateToFollowers(String topic, Partition partition, byte[] message, long offset) {
        for (int followerId : partition.getFollowers()) {
            if (followerId == brokerId)
                continue; // Skip self

            BrokerInfo follower = clusterMetadata.get(followerId);
            if (follower == null)
                continue;

            executor.submit(() -> {
                try {
                    byte ack = Protocol.call(follower.getHost(), follower.getPort(),
                            Protocol.encodeReplicateRequest(topic, partition.getId(), offset, message)).get();
                    LOGGER.info("Replication to follower " + followerId + " " +
                            (ack == Protocol.REPLICATE_ACK ? "succeeded" : "failed"));
                } catch (IOException e) {
                    LOGGER.log(Level.SEVERE, "Replication to follower " + followerId + " failed", e);
                }
            });
        }
    }

    /**
     * Handle replication request from leader
     */
    private void handleReplicateRequest(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        String topic = Protocol.readString(buffer);

        int partitionId = buffer.getInt();
        long offset = buffer.getLong();
        int messageSize = buffer.getInt();
        byte[] message = new byte[messageSize];
        buffer.get(message);

        LOGGER.info("Replication request for topic: " + topic + ", partition: " + partitionId + ", offset: " + offset);

        // Check if topic exists
        if (!topics.containsKey(topic)) {
            ByteBuffer response = ByteBuffer.allocate(1);
            response.put((byte) 0); // Failed
            response.flip();
            Protocol.writeFrame(clientChannel, response);
            return;
        }

        // Find the partition
        List<Partition> partitions = topics.get(topic);
        Partition targetPartition = null;

        for (Partition p : partitions) {
            if (p.getId() == partitionId) {
                targetPartition = p;
                break;
            }
        }

        if (targetPartition == null) {
            ByteBuffer response = ByteBuffer.allocate(1);
            response.put((byte) 0); // Failed
            response.flip();
            Protocol.writeFrame(clientChannel, response);
            return;
        }

        // Append message to log (as follower)
        long appendedOffset = targetPartition.append(message);

        // Send acknowledgment
        ByteBuffer response = ByteBuffer.allocate(1);
        response.put(appendedOffset >= 0 ? Protocol.REPLICATE_ACK : (byte) 0);
        response.flip();
        Protocol.writeFrame(clientChannel, response);
    }

    /**
     * Handle fetch request from client
     */
    private void handleFetchRequest(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        String topic = Protocol.readString(buffer);

        int partition = buffer.getInt();
        long offset = buffer.getLong();
        int maxBytes = buffer.getInt();

        LOGGER.info("Fetch request for topic: " + topic + ", partition: " + partition +
                ", offset: " + offset + ", maxBytes: " + maxBytes);

        // Check if topic exists
        if (!topics.containsKey(topic)) {
            Protocol.sendErrorResponse(clientChannel, "Topic does not exist");
            return;
        }

        // Find the partition
        List<Partition> partitions = topics.get(topic);
        Partition targetPartition = null;

        for (Partition p : partitions) {
            if (p.getId() == partition) {
                targetPartition = p;
                break;
            }
        }

        if (targetPartition == null) {
            Protocol.sendErrorResponse(clientChannel, "Partition does not exist");
            return;
        }

        // A follower can lag the leader, so a reader with stale metadata must refresh rather than read here.
        if (targetPartition.getLeader() != brokerId) {
            Protocol.sendErrorResponse(clientChannel, "Not the leader for partition " + partition);
            return;
        }

        List<byte[]> messages = offset >= targetPartition.getLogEndOffset()
                ? List.of()
                : targetPartition.readMessages(offset, Math.min(maxBytes, Protocol.MAX_FRAME_BYTES));
        Protocol.writeFrame(clientChannel, Protocol.encodeFetchResponse(offset, messages));
    }

    /**
     * Handle metadata request from client
     */
    private void handleMetadataRequest(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        // Topics and brokers can be added while this runs, so write each count from the same copy as its entries.
        List<BrokerInfo> brokers = new ArrayList<>(clusterMetadata.values());
        List<Map.Entry<String, List<Partition>>> topicEntries = new ArrayList<>(topics.entrySet());

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream response = new DataOutputStream(bytes);
        response.writeByte(Protocol.METADATA_RESPONSE);

        response.writeInt(brokers.size());
        for (BrokerInfo broker : brokers) {
            response.writeInt(broker.getId());
            byte[] host = broker.getHost().getBytes(StandardCharsets.UTF_8);
            response.writeShort(host.length);
            response.write(host);
            response.writeInt(broker.getPort());
        }

        response.writeInt(topicEntries.size());
        for (Map.Entry<String, List<Partition>> entry : topicEntries) {
            byte[] topicBytes = entry.getKey().getBytes(StandardCharsets.UTF_8);
            response.writeShort(topicBytes.length);
            response.write(topicBytes);
            response.writeInt(entry.getValue().size());

            for (Partition partition : entry.getValue()) {
                response.writeInt(partition.getId());
                response.writeInt(partition.getLeader());

                List<Integer> followers = partition.getFollowers();
                response.writeInt(followers.size());
                for (int follower : followers) {
                    response.writeInt(follower);
                }
            }
        }

        Protocol.writeFrame(clientChannel, ByteBuffer.wrap(bytes.toByteArray()));
    }

    /**
     * Handle create topic request from client
     */
    private void handleCreateTopicRequest(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        String topic = Protocol.readString(buffer);

        if (!isValidTopicName(topic)) {
            Protocol.sendErrorResponse(clientChannel, "Invalid topic name");
            return;
        }

        int numPartitions = buffer.getInt();
        short replicationFactor = buffer.getShort();

        LOGGER.info("Create topic request: " + topic +
                ", partitions: " + numPartitions +
                ", replication: " + replicationFactor);

        // Check if topic already exists
        if (topics.containsKey(topic)) {
            Protocol.sendErrorResponse(clientChannel, "Topic already exists");
            return;
        }

        if (numPartitions <= 0 || replicationFactor <= 0) {
            Protocol.sendErrorResponse(clientChannel, "Invalid topic configuration");
            return;
        }

        // As controller, create the topic
        if (isController.get()) {
            ByteBuffer response;
            try {
                response = clusterEvents.submit(() -> createTopicAsController(topic, numPartitions, replicationFactor))
                        .get();
            } catch (RejectedExecutionException | ExecutionException e) {
                LOGGER.log(Level.SEVERE, "Failed to create topic", e);
                response = Protocol.encodeErrorResponse("Failed to create topic");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            Protocol.writeFrame(clientChannel, response);
        } else {
            // Forward to controller
            forwardCreateTopicToController(clientChannel, topic, numPartitions, replicationFactor);
        }
    }

    /**
     * Check and create a topic on clusterEvents, and return the reply. The checks, the partition assignment and
     * the brokers told about the topic all use the brokers registered in ZooKeeper when this runs.
     */
    private ByteBuffer createTopicAsController(String topic, int numPartitions, short replicationFactor)
            throws IOException, KeeperException, InterruptedException {
        refreshBrokers();
        if (replicationFactor > clusterMetadata.size()) {
            return Protocol.encodeErrorResponse("Invalid topic configuration");
        }
        int brokerIdWidth = clusterMetadata.keySet().stream().mapToInt(id -> String.valueOf(id).length()).max().orElse(1);
        if (createTopicRequestBytes(topic, numPartitions, replicationFactor, brokerIdWidth) > ZOOKEEPER_MAX_REQUEST_BYTES) {
            return Protocol.encodeErrorResponse("Too many partitions: the topic's ZooKeeper nodes exceed the "
                    + ZOOKEEPER_MAX_REQUEST_BYTES + "-byte limit of one ZooKeeper request");
        }
        if (nameTakenIgnoringCase(topic)) {
            return Protocol.encodeErrorResponse("Topic already exists");
        }
        boolean created = createTopic(topic, numPartitions, replicationFactor);
        ByteBuffer status = ByteBuffer.allocate(2);
        status.put(Protocol.CREATE_TOPIC_RESPONSE);
        status.put((byte) (created ? 0 : 1));
        status.flip();
        return status;
    }

    /**
     * Names that differ only in case share a partition directory on a case-insensitive filesystem,
     * so a name is taken when it matches a known topic or a ZooKeeper topic ignoring case.
     */
    private boolean nameTakenIgnoringCase(String topic) throws IOException {
        List<String> names = new ArrayList<>(topics.keySet());
        try {
            names.addAll(zkClient.getChildren("/topics"));
        } catch (KeeperException | InterruptedException e) {
            throw new IOException("Failed to list topics in ZooKeeper", e);
        }
        return names.stream().anyMatch(topic::equalsIgnoreCase);
    }

    /**
     * Forward create topic request to controller
     */
    private void forwardCreateTopicToController(SocketChannel clientChannel, String topic,
            int numPartitions, short replicationFactor) throws IOException {
        // Find controller
        int controllerId = -1;
        try {
            String controllerData = zkClient.getData("/controller");
            controllerId = Integer.parseInt(controllerData);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to get controller info", e);
            Protocol.sendErrorResponse(clientChannel, "Controller not available");
            return;
        }

        // This broker is not the controller, so a /controller naming it is left by its own dead session.
        if (controllerId == brokerId) {
            Protocol.sendErrorResponse(clientChannel, "Controller not available");
            return;
        }

        BrokerInfo controller = clusterMetadata.get(controllerId);
        if (controller == null) {
            Protocol.sendErrorResponse(clientChannel, "Controller broker not available");
            return;
        }

        ByteBuffer response;
        try {
            response = Protocol.call(controller.getHost(), controller.getPort(),
                    Protocol.encodeCreateTopicRequest(topic, numPartitions, replicationFactor));
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to forward create topic request to controller", e);
            Protocol.sendErrorResponse(clientChannel, "Failed to forward to controller");
            return;
        }
        Protocol.writeFrame(clientChannel, response);
    }

    /**
     * A topic name becomes a directory and a ZooKeeper path, so it may hold only ASCII letters, digits,
     * dots, underscores and hyphens, at most 249 characters, and may not be "." or "..". Non-ASCII is
     * out because filesystems that fold Unicode case or normalization map distinct names to one directory.
     */
    static boolean isValidTopicName(String topic) {
        if (topic.isEmpty() || topic.length() > 249 || topic.equals(".") || topic.equals("..")) {
            return false;
        }
        return topic.chars().allMatch(c -> (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
                || c == '.' || c == '_' || c == '-');
    }

    /** ZooKeeper's default jute.maxbuffer. The server closes the connection on a longer request. */
    static final int ZOOKEEPER_MAX_REQUEST_BYTES = 0xfffff;

    /**
     * An upper bound on the size of the one ZooKeeper multi request that creates a topic, as ZooKeeper 3.8 encodes
     * it after the length prefix: a request header (8 bytes) and a closing op header (9), plus per created node
     * an op header (9), the path and data each with a 4-byte length, the open ACL (27) and flags (4). The topic
     * and partitions nodes have no data. A partition node's data names each replica as its id and a separator.
     */
    static long createTopicRequestBytes(String topic, int numPartitions, short replicationFactor, int brokerIdWidth) {
        int perNode = 9 + 4 + 4 + 27 + 4;
        int topicNode = perNode + "/topics/".length() + topic.length();
        int partitionsNode = topicNode + "/partitions".length();
        int partitionNode = partitionsNode + "/".length() + String.valueOf(numPartitions - 1).length()
                + replicationFactor * (brokerIdWidth + 1);
        return 8 + 9 + topicNode + partitionsNode + (long) numPartitions * partitionNode;
    }

    /**
     * Create a new topic with the specified configuration
     */
    private boolean createTopic(String topic, int numPartitions, short replicationFactor) {
        if (!isController.get()) {
            LOGGER.warning("Only the controller can create topics");
            return false;
        }

        try {
            // Create topic directory
            String topicDir = DATA_DIR + File.separator + brokerId + File.separator + topic;
            new File(topicDir).mkdirs();

            // Create topic in ZooKeeper
            String topicPath = "/topics/" + topic;
            if (zkClient.exists(topicPath)) {
                LOGGER.warning("Topic already exists in ZooKeeper: " + topic);
                return false;
            }
            Map<String, String> nodes = new LinkedHashMap<>();
            nodes.put(topicPath, "");
            nodes.put(topicPath + "/partitions", "");

            // Create partitions
            List<Partition> partitions = new ArrayList<>();
            List<Integer> brokerIds = new ArrayList<>(clusterMetadata.keySet());

            for (int i = 0; i < numPartitions; i++) {
                int partitionId = i;
                String partitionDir = topicDir + File.separator + partitionId;
                new File(partitionDir).mkdirs();

                // Select leader and followers
                int leaderIndex = i % brokerIds.size();
                int leaderId = brokerIds.get(leaderIndex);

                List<Integer> followers = new ArrayList<>();
                for (int j = 1; j < replicationFactor; j++) {
                    int followerIndex = (leaderIndex + j) % brokerIds.size();
                    followers.add(brokerIds.get(followerIndex));
                }

                // Create partition
                Partition partition = new Partition(partitionId, leaderId, followers, partitionDir);
                partitions.add(partition);

                // Store partition metadata in ZooKeeper
                String partitionPath = topicPath + "/partitions/" + partitionId;
                String partitionData = leaderId + ";";
                for (int follower : followers) {
                    partitionData += follower + ",";
                }

                nodes.put(partitionPath, partitionData);
            }
            zkClient.createPersistentNodes(nodes);
            LOGGER.info("Created topic " + topic + " with " + numPartitions + " partitions");

            // Add topic to broker's metadata
            topics.put(topic, partitions);

            // Reply only after the other brokers load the topic, so a produce to any partition right after
            // success finds it. A broker that misses the deadline still gets success, because the topic is
            // already in ZooKeeper and the broker loads it from there when it restarts.
            Map<Integer, Future<Boolean>> acks = new HashMap<>();
            for (int brokerId : brokerIds) {
                if (brokerId != this.brokerId) {
                    acks.put(brokerId, notifyBrokerOfTopic(brokerId, topic));
                }
            }
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            for (Map.Entry<Integer, Future<Boolean>> ack : acks.entrySet()) {
                boolean loaded;
                try {
                    loaded = ack.getValue().get(deadline - System.nanoTime(), TimeUnit.NANOSECONDS);
                } catch (ExecutionException | TimeoutException e) {
                    loaded = false;
                }
                if (!loaded) {
                    LOGGER.warning("Broker " + ack.getKey() + " did not confirm loading topic " + topic + " within 5 s");
                }
            }
            return true;
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to create topic", e);
            return false;
        }
    }
}
