package com.simplekafka.client;

import java.io.IOException;
import java.net.ConnectException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.simplekafka.broker.BrokerInfo;
import com.simplekafka.broker.Protocol;

/**
 * Client for interacting with Build Your Own Kafka brokers
 */
public class SimpleKafkaClient {
    private static final Logger LOGGER = Logger.getLogger(SimpleKafkaClient.class.getName());
    private static final long TOPIC_VISIBLE_TIMEOUT_NANOS = java.util.concurrent.TimeUnit.SECONDS.toNanos(5);

    private final String bootstrapBroker;
    private final int bootstrapPort;
    private final AtomicInteger correlationId;
    private volatile Snapshot snapshot = new Snapshot(Map.of(), Map.of());

    /**
     * Create a SimpleKafka client
     * 
     * @param bootstrapBroker Host of a broker to connect to
     * @param bootstrapPort   Port of the broker to connect to
     */
    public SimpleKafkaClient(String bootstrapBroker, int bootstrapPort) {
        this.bootstrapBroker = bootstrapBroker;
        this.bootstrapPort = bootstrapPort;
        this.correlationId = new AtomicInteger(0);
    }

    /**
     * Initialize the client by fetching cluster metadata
     */
    public void initialize() throws IOException {
        refreshMetadata();
    }

    /**
     * Refresh metadata about brokers and topics
     */
    public void refreshMetadata() throws IOException {
        ByteBuffer response = Protocol.call(bootstrapBroker, bootstrapPort, Protocol.encodeMetadataRequest());
        Protocol.MetadataResult result = Protocol.decodeMetadataResponse(response);

        if (!result.isSuccess()) {
            throw new IOException("Failed to fetch metadata: " + result.getError());
        }

        Map<Integer, BrokerInfo> brokers = new HashMap<>();
        for (BrokerInfo broker : result.getBrokers()) {
            brokers.put(broker.getId(), new BrokerInfo(broker.getId(), broker.getHost(), broker.getPort()));
        }

        Map<String, TopicMetadata> topics = new HashMap<>();
        for (Protocol.TopicMetadata topic : result.getTopics()) {
            List<PartitionInfo> partitions = new ArrayList<>();

            for (Protocol.PartitionMetadata partition : topic.getPartitions()) {
                partitions.add(new PartitionInfo(
                        partition.getId(),
                        partition.getLeader(),
                        partition.getReplicas()));
            }

            topics.put(topic.getName(), new TopicMetadata(topic.getName(), partitions));
        }
        snapshot = new Snapshot(brokers, topics);

        LOGGER.info("Metadata refreshed: " + brokers.size() + " brokers, " +
                topics.size() + " topics");
    }

    /**
     * Create a new topic
     */
    public boolean createTopic(String topic, int numPartitions, short replicationFactor) throws IOException {
        // Find a broker to send the request to (prefer controller if known)
        Map<Integer, BrokerInfo> brokers = snapshot.brokers;
        if (brokers.isEmpty()) {
            refreshMetadata();
            brokers = snapshot.brokers;
            if (brokers.isEmpty()) {
                throw new IOException("No brokers available");
            }
        }

        BrokerInfo broker = brokers.values().iterator().next();

        ByteBuffer response = Protocol.call(broker.getHost(), broker.getPort(),
                Protocol.encodeCreateTopicRequest(topic, numPartitions, replicationFactor));

        byte responseType = response.get();
        if (responseType != Protocol.CREATE_TOPIC_RESPONSE) {
            if (responseType == Protocol.ERROR_RESPONSE) {
                String error = Protocol.readString(response);
                LOGGER.warning("Error creating topic: " + error);
                return false;
            }
            throw new IOException("Invalid create topic response type: " + responseType);
        }

        byte status = response.get();
        boolean success = status == 0;

        if (success) {
            awaitTopicInMetadata(topic);
        }

        return success;
    }

    /**
     * The controller tells other brokers about a new topic after it replies, so the broker this client
     * reads metadata from may not know the topic yet.
     */
    private void awaitTopicInMetadata(String topic) throws IOException {
        long deadline = System.nanoTime() + TOPIC_VISIBLE_TIMEOUT_NANOS;
        refreshMetadata();
        while (!snapshot.topics.containsKey(topic) && System.nanoTime() < deadline) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted waiting for topic " + topic, e);
            }
            refreshMetadata();
        }
        if (!snapshot.topics.containsKey(topic)) {
            throw new IOException("Topic " + topic + " was created but is not in metadata after 5 s");
        }
    }

    /**
     * Produce a message to a topic-partition
     */
    public long send(String topic, int partition, byte[] message) throws IOException {
        try {
            return sendOnce(topic, partition, message);
        } catch (ConnectException e) {
            // Retry only when nothing reached the broker, so a retry cannot append twice.
            LOGGER.warning("Leader unreachable, refreshing metadata and retrying: " + e);
            refreshMetadata();
            return sendOnce(topic, partition, message);
        }
    }

    private long sendOnce(String topic, int partition, byte[] message) throws IOException {
        TopicMetadata metadata = snapshot.topics.get(topic);
        if (metadata == null) {
            refreshMetadata();
            metadata = snapshot.topics.get(topic);
            if (metadata == null) {
                throw new IOException("Topic not found: " + topic);
            }
        }

        // Find the leader for the partition
        PartitionInfo partitionInfo = null;

        for (PartitionInfo info : metadata.getPartitions()) {
            if (info.getId() == partition) {
                partitionInfo = info;
                break;
            }
        }

        if (partitionInfo == null) {
            throw new IOException("Partition not found: " + partition);
        }

        int leaderId = partitionInfo.getLeader();
        BrokerInfo leader = snapshot.brokers.get(leaderId);

        if (leader == null) {
            refreshMetadata();
            leader = snapshot.brokers.get(leaderId);

            if (leader == null) {
                throw new IOException("Leader broker not found: " + leaderId);
            }
        }

        // Send to leader
        ByteBuffer response = Protocol.call(leader.getHost(), leader.getPort(),
                Protocol.encodeProduceRequest(topic, partition, message));
        Protocol.ProduceResult result = Protocol.decodeProduceResponse(response);

        if (!result.isSuccess()) {
            throw new IOException("Failed to produce message: " + result.getError());
        }

        return result.getOffset();
    }

    /**
     * Consume messages from a topic-partition
     */
    public List<byte[]> fetch(String topic, int partition, long offset, int maxBytes) throws IOException {
        try {
            return fetchOnce(topic, partition, offset, maxBytes);
        } catch (IOException e) {
            LOGGER.warning("Fetch failed, refreshing metadata and retrying: " + e);
            refreshMetadata();
            return fetchOnce(topic, partition, offset, maxBytes);
        }
    }

    private List<byte[]> fetchOnce(String topic, int partition, long offset, int maxBytes) throws IOException {
        TopicMetadata metadata = snapshot.topics.get(topic);
        if (metadata == null) {
            refreshMetadata();
            metadata = snapshot.topics.get(topic);
            if (metadata == null) {
                throw new IOException("Topic not found: " + topic);
            }
        }

        // Find the leader for the partition
        PartitionInfo partitionInfo = null;

        for (PartitionInfo info : metadata.getPartitions()) {
            if (info.getId() == partition) {
                partitionInfo = info;
                break;
            }
        }

        if (partitionInfo == null) {
            throw new IOException("Partition not found: " + partition);
        }

        int leaderId = partitionInfo.getLeader();
        BrokerInfo leader = snapshot.brokers.get(leaderId);

        if (leader == null) {
            refreshMetadata();
            leader = snapshot.brokers.get(leaderId);

            if (leader == null) {
                throw new IOException("Leader broker not found: " + leaderId);
            }
        }

        // Fetch from leader
        ByteBuffer response = Protocol.call(leader.getHost(), leader.getPort(),
                Protocol.encodeFetchRequest(topic, partition, offset, maxBytes));
        Protocol.FetchResult result = Protocol.decodeFetchResponse(response);

        if (!result.isSuccess()) {
            throw new IOException("Failed to fetch messages: " + result.getError());
        }

        List<byte[]> messages = new ArrayList<>();
        for (byte[] msg : result.getMessages()) {
            messages.add(msg);
        }

        return messages;
    }

    /**
     * Get metadata for all topics
     */
    public Map<String, TopicMetadata> getTopicMetadata() {
        return new HashMap<>(snapshot.topics);
    }

    /**
     * Get metadata for a specific topic
     */
    public TopicMetadata getTopicMetadata(String topic) {
        return snapshot.topics.get(topic);
    }

    /**
     * Get broker information
     */
    public Map<Integer, BrokerInfo> getBrokers() {
        return new HashMap<>(snapshot.brokers);
    }

    /**
     * One metadata response. A refresh replaces the whole snapshot, so a reader never sees it half built.
     */
    private static final class Snapshot {
        final Map<Integer, BrokerInfo> brokers;
        final Map<String, TopicMetadata> topics;

        Snapshot(Map<Integer, BrokerInfo> brokers, Map<String, TopicMetadata> topics) {
            this.brokers = Map.copyOf(brokers);
            this.topics = Map.copyOf(topics);
        }
    }

    /**
     * Topic metadata
     */
    public static class TopicMetadata {
        private final String name;
        private final List<PartitionInfo> partitions;

        public TopicMetadata(String name, List<PartitionInfo> partitions) {
            this.name = name;
            this.partitions = partitions;
        }

        public String getName() {
            return name;
        }

        public List<PartitionInfo> getPartitions() {
            return new ArrayList<>(partitions);
        }

        @Override
        public String toString() {
            return "TopicMetadata{name='" + name + "', partitions=" + partitions + "}";
        }
    }

    /**
     * Partition information
     */
    public static class PartitionInfo {
        private final int id;
        private final int leader;
        private final List<Integer> followers;

        public PartitionInfo(int id, int leader, List<Integer> followers) {
            this.id = id;
            this.leader = leader;
            this.followers = followers;
        }

        public int getId() {
            return id;
        }

        public int getLeader() {
            return leader;
        }

        public List<Integer> getFollowers() {
            return new ArrayList<>(followers);
        }

        @Override
        public String toString() {
            return "PartitionInfo{id=" + id + ", leader=" + leader + ", followers=" + followers + "}";
        }
    }
}
