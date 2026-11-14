package com.simplekafka.broker;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ProtocolException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Defines the wire protocol for Build Your Own Kafka
 */
public class Protocol {
    // Client request types
    public static final byte PRODUCE = 0x01;
    public static final byte FETCH = 0x02;
    public static final byte METADATA = 0x03;
    public static final byte CREATE_TOPIC = 0x04;

    // Broker response types
    public static final byte PRODUCE_RESPONSE = 0x11;
    public static final byte FETCH_RESPONSE = 0x12;
    public static final byte METADATA_RESPONSE = 0x13;
    public static final byte CREATE_TOPIC_RESPONSE = 0x14;
    public static final byte ERROR_RESPONSE = 0x1F;

    // Internal broker communication
    public static final byte REPLICATE = 0x21;
    public static final byte REPLICATE_ACK = 0x22;
    public static final byte TOPIC_NOTIFICATION = 0x23;

    public static final int MAX_FRAME_BYTES = 16 * 1024 * 1024;

    /**
     * Write one frame: a 4-byte big-endian body length, then the body.
     */
    public static void writeFrame(WritableByteChannel channel, ByteBuffer body) throws IOException {
        ByteBuffer frame = ByteBuffer.allocate(4 + body.remaining());
        frame.putInt(body.remaining()).put(body).flip();
        while (frame.hasRemaining()) {
            channel.write(frame);
        }
    }

    /**
     * Read one frame and return its body, or null if the peer closed before sending a byte.
     * Throws ProtocolException for a size outside 1..MAX_FRAME_BYTES and EOFException for a
     * frame cut short.
     */
    public static ByteBuffer readFrame(ReadableByteChannel channel) throws IOException {
        ByteBuffer header = ByteBuffer.allocate(4);
        if (channel.read(header) < 0) {
            return null;
        }
        readFully(channel, header);
        int size = header.flip().getInt();
        if (size < 1 || size > MAX_FRAME_BYTES) {
            throw new ProtocolException("Frame size " + size + " is outside 1.." + MAX_FRAME_BYTES);
        }
        ByteBuffer body = ByteBuffer.allocate(size);
        readFully(channel, body);
        return body.flip();
    }

    private static void readFully(ReadableByteChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            if (channel.read(buffer) < 0) {
                throw new EOFException("Connection closed mid-frame");
            }
        }
    }

    /**
     * Send one request frame on a new connection and return the response body.
     */
    public static ByteBuffer call(String host, int port, ByteBuffer request) throws IOException {
        try (SocketChannel channel = SocketChannel.open(new InetSocketAddress(host, port))) {
            writeFrame(channel, request);
            ByteBuffer response = readFrame(channel);
            if (response == null) {
                throw new IOException("No data received from broker");
            }
            return response;
        }
    }

    /**
     * Read a 2-byte length followed by that many UTF-8 bytes.
     */
    public static String readString(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.getShort()];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * Send an error response to the client
     */
    public static void sendErrorResponse(SocketChannel channel, String errorMessage) throws IOException {
        byte[] error = errorMessage.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(3 + error.length);
        buffer.put(ERROR_RESPONSE);
        buffer.putShort((short) error.length);
        buffer.put(error);
        buffer.flip();
        writeFrame(channel, buffer);
    }

    /**
     * Encode a producer request
     */
    public static ByteBuffer encodeProduceRequest(String topic, int partition, byte[] message) {
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(11 + topicBytes.length + message.length);
        buffer.put(PRODUCE);
        buffer.putShort((short) topicBytes.length);
        buffer.put(topicBytes);
        buffer.putInt(partition);
        buffer.putInt(message.length);
        buffer.put(message);
        buffer.flip();
        return buffer;
    }

    /**
     * Encode a fetch request
     */
    public static ByteBuffer encodeFetchRequest(String topic, int partition, long offset, int maxBytes) {
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(19 + topicBytes.length);
        buffer.put(FETCH);
        buffer.putShort((short) topicBytes.length);
        buffer.put(topicBytes);
        buffer.putInt(partition);
        buffer.putLong(offset);
        buffer.putInt(maxBytes);
        buffer.flip();
        return buffer;
    }

    /**
     * Encode a metadata request
     */
    public static ByteBuffer encodeMetadataRequest() {
        ByteBuffer buffer = ByteBuffer.allocate(1);
        buffer.put(METADATA);
        buffer.flip();
        return buffer;
    }

    /**
     * Encode a create topic request
     */
    public static ByteBuffer encodeCreateTopicRequest(String topic, int numPartitions, short replicationFactor) {
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(9 + topicBytes.length);
        buffer.put(CREATE_TOPIC);
        buffer.putShort((short) topicBytes.length);
        buffer.put(topicBytes);
        buffer.putInt(numPartitions);
        buffer.putShort(replicationFactor);
        buffer.flip();
        return buffer;
    }

    /**
     * Encode a replication request
     */
    public static ByteBuffer encodeReplicateRequest(String topic, int partition, long offset, byte[] message) {
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(19 + topicBytes.length + message.length);
        buffer.put(REPLICATE);
        buffer.putShort((short) topicBytes.length);
        buffer.put(topicBytes);
        buffer.putInt(partition);
        buffer.putLong(offset);
        buffer.putInt(message.length);
        buffer.put(message);
        buffer.flip();
        return buffer;
    }

    /**
     * Encode a topic notification
     */
    public static ByteBuffer encodeTopicNotification(String topic) {
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(3 + topicBytes.length);
        buffer.put(TOPIC_NOTIFICATION);
        buffer.putShort((short) topicBytes.length);
        buffer.put(topicBytes);
        buffer.flip();
        return buffer;
    }

    /**
     * Decode a produce response
     */
    public static ProduceResult decodeProduceResponse(ByteBuffer buffer) {
        byte responseType = buffer.get();
        if (responseType != PRODUCE_RESPONSE) {
            if (responseType == ERROR_RESPONSE) {
                String error = readString(buffer);
                return new ProduceResult(-1, error);
            }
            return new ProduceResult(-1, "Invalid response type");
        }

        long offset = buffer.getLong();
        byte status = buffer.get();

        return new ProduceResult(offset, status == 0 ? null : "Produce failed");
    }

    /**
     * Decode a fetch response
     */
    public static FetchResult decodeFetchResponse(ByteBuffer buffer) {
        byte responseType = buffer.get();
        if (responseType != FETCH_RESPONSE) {
            if (responseType == ERROR_RESPONSE) {
                String error = readString(buffer);
                return new FetchResult(new byte[0][], error);
            }
            return new FetchResult(new byte[0][], "Invalid response type");
        }

        int messageCount = buffer.getInt();
        byte[][] messages = new byte[messageCount][];

        for (int i = 0; i < messageCount; i++) {
            long offset = buffer.getLong(); // Skip offset
            int messageSize = buffer.getInt();
            messages[i] = new byte[messageSize];
            buffer.get(messages[i]);
        }

        return new FetchResult(messages, null);
    }

    /**
     * Decode metadata response
     */
    public static MetadataResult decodeMetadataResponse(ByteBuffer buffer) {
        byte responseType = buffer.get();
        if (responseType != METADATA_RESPONSE) {
            if (responseType == ERROR_RESPONSE) {
                String error = readString(buffer);
                return new MetadataResult(new ArrayList<>(), new ArrayList<>(), error);
            }
            return new MetadataResult(new ArrayList<>(), new ArrayList<>(), "Invalid response type");
        }

        // Parse broker info
        int brokerCount = buffer.getInt();
        List<BrokerInfo> brokers = new ArrayList<>();

        for (int i = 0; i < brokerCount; i++) {
            int brokerId = buffer.getInt();
            String host = readString(buffer);
            int port = buffer.getInt();

            brokers.add(new BrokerInfo(brokerId, host, port));
        }

        // Parse topic metadata
        int topicCount = buffer.getInt();
        List<TopicMetadata> topics = new ArrayList<>();

        for (int i = 0; i < topicCount; i++) {
            String topicName = readString(buffer);

            int partitionCount = buffer.getInt();
            List<PartitionMetadata> partitions = new ArrayList<>();

            for (int j = 0; j < partitionCount; j++) {
                int partitionId = buffer.getInt();
                int leaderId = buffer.getInt();

                int replicas = buffer.getInt();
                List<Integer> replicaIds = new ArrayList<>();

                for (int k = 0; k < replicas; k++) {
                    replicaIds.add(buffer.getInt());
                }

                partitions.add(new PartitionMetadata(partitionId, leaderId, replicaIds));
            }

            topics.add(new TopicMetadata(topicName, partitions));
        }

        return new MetadataResult(brokers, topics, null);
    }

    /**
     * Result class for produce operations
     */
    public static class ProduceResult {
        private final long offset;
        private final String error;

        public ProduceResult(long offset, String error) {
            this.offset = offset;
            this.error = error;
        }

        public long getOffset() {
            return offset;
        }

        public String getError() {
            return error;
        }

        public boolean isSuccess() {
            return error == null;
        }
    }

    /**
     * Result class for fetch operations
     */
    public static class FetchResult {
        private final byte[][] messages;
        private final String error;

        public FetchResult(byte[][] messages, String error) {
            this.messages = messages;
            this.error = error;
        }

        public byte[][] getMessages() {
            return messages;
        }

        public int getMessageCount() {
            return messages.length;
        }

        public String getError() {
            return error;
        }

        public boolean isSuccess() {
            return error == null;
        }
    }

    /**
     * Result class for metadata operations
     */
    public static class MetadataResult {
        private final List<BrokerInfo> brokers;
        private final List<TopicMetadata> topics;
        private final String error;

        public MetadataResult(List<BrokerInfo> brokers, List<TopicMetadata> topics, String error) {
            this.brokers = brokers;
            this.topics = topics;
            this.error = error;
        }

        public List<BrokerInfo> getBrokers() {
            return brokers;
        }

        public List<TopicMetadata> getTopics() {
            return topics;
        }

        public String getError() {
            return error;
        }

        public boolean isSuccess() {
            return error == null;
        }
    }

    /**
     * Topic metadata class
     */
    public static class TopicMetadata {
        private final String name;
        private final List<PartitionMetadata> partitions;

        public TopicMetadata(String name, List<PartitionMetadata> partitions) {
            this.name = name;
            this.partitions = partitions;
        }

        public String getName() {
            return name;
        }

        public List<PartitionMetadata> getPartitions() {
            return partitions;
        }
    }

    /**
     * Partition metadata class
     */
    public static class PartitionMetadata {
        private final int id;
        private final int leader;
        private final List<Integer> replicas;

        public PartitionMetadata(int id, int leader, List<Integer> replicas) {
            this.id = id;
            this.leader = leader;
            this.replicas = replicas;
        }

        public int getId() {
            return id;
        }

        public int getLeader() {
            return leader;
        }

        public List<Integer> getReplicas() {
            return replicas;
        }
    }
}
