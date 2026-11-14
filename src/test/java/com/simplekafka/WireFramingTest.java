package com.simplekafka;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.simplekafka.broker.Protocol;
import com.simplekafka.broker.ZookeeperClient;
import com.simplekafka.client.SimpleKafkaClient;

/**
 * Every request and response is {@code int32 size | size bytes}. The raw-socket cases write
 * that prefix by hand, so the test states the wire format independently of Protocol.
 */
class WireFramingTest {
    private static final int MAX_FRAME_BYTES = 16 * 1024 * 1024;

    private TestCluster cluster;

    @BeforeEach
    void startCluster() throws Exception {
        cluster = TestCluster.start(1);
    }

    @AfterEach
    void stopCluster() {
        cluster.close();
    }

    @Test
    void produceOfMessageOver1024BytesSucceeds() throws Exception {
        SimpleKafkaClient client = cluster.client(1);
        createTopic(client, "big-produce", 1);
        byte[] message = payload(2000, 'a');

        assertEquals(0L, client.send("big-produce", 0, message));
        assertArrayEquals(message, client.fetch("big-produce", 0, 0, 1024 * 1024).get(0));
    }

    @Test
    void fetchResponseOver4KbDecodes() throws Exception {
        SimpleKafkaClient client = cluster.client(1);
        createTopic(client, "big-fetch", 1);
        for (int i = 0; i < 10; i++) {
            client.send("big-fetch", 0, payload(1000, (char) ('0' + i)));
        }

        List<byte[]> fetched = client.fetch("big-fetch", 0, 0, 1024 * 1024);
        assertEquals(10, fetched.size());
        for (int i = 0; i < 10; i++) {
            assertArrayEquals(payload(1000, (char) ('0' + i)), fetched.get(i));
        }
    }

    @Test
    void nonAsciiTopicNameRoundTrips() throws Exception {
        SimpleKafkaClient client = cluster.client(1);
        String topic = "café-日本";
        createTopic(client, topic, 1);

        byte[] message = "héllo".getBytes(StandardCharsets.UTF_8);
        assertEquals(0L, client.send(topic, 0, message));
        assertArrayEquals(message, client.fetch(topic, 0, 0, 1024 * 1024).get(0));
    }

    @Test
    void metadataForTopicWith400PartitionsDecodes() throws Exception {
        createTopic(cluster.client(1), "many", 400);

        assertEquals(400, cluster.client(1).getTopicMetadata("many").getPartitions().size());
    }

    @Test
    void requestSplitAcrossTwoTcpWritesIsAnswered() throws Exception {
        createTopic(cluster.client(1), "split", 1);
        byte[] frame = frame(Protocol.encodeProduceRequest("split", 0, payload(10, 's')));

        try (Socket socket = rawSocket(1)) {
            OutputStream out = socket.getOutputStream();
            out.write(frame, 0, 5);
            out.flush();
            Thread.sleep(300);
            out.write(frame, 5, frame.length - 5);
            out.flush();

            Protocol.ProduceResult result = Protocol.decodeProduceResponse(readFrame(socket.getInputStream()));
            assertTrue(result.isSuccess(), String.valueOf(result.getError()));
            assertEquals(0L, result.getOffset());
        }
    }

    @Test
    void twoRequestsInOneWriteBothGetAnswered() throws Exception {
        byte[] metadata = frame(Protocol.encodeMetadataRequest());
        byte[] both = Arrays.copyOf(metadata, metadata.length * 2);
        System.arraycopy(metadata, 0, both, metadata.length, metadata.length);

        try (Socket socket = rawSocket(1)) {
            socket.getOutputStream().write(both);
            for (int i = 0; i < 2; i++) {
                assertTrue(Protocol.decodeMetadataResponse(readFrame(socket.getInputStream())).isSuccess(),
                        "metadata response " + i);
            }
        }
    }

    @Test
    void oversizeRequestGetsErrorFrameThenClose() throws Exception {
        try (Socket socket = rawSocket(1)) {
            socket.getOutputStream().write(ByteBuffer.allocate(4).putInt(MAX_FRAME_BYTES + 1).array());

            assertEquals(Protocol.ERROR_RESPONSE, readFrame(socket.getInputStream()).get());
            assertEquals(-1, socket.getInputStream().read(), "broker closes after an oversize frame");
        }
    }

    @Test
    void controllerRejectionIsForwardedWhole() throws Exception {
        createTopic(cluster.client(1), "dup", 1);
        forgetTopicInZookeeper("dup");
        cluster.addBroker(2);
        TestCluster.await("broker 2 sees the controller", () -> {
            try {
                return cluster.client(2).getBrokers().size() == 2;
            } catch (IOException e) {
                return false;
            }
        });

        try (Socket socket = rawSocket(2)) {
            socket.getOutputStream().write(frame(Protocol.encodeCreateTopicRequest("dup", 1, (short) 1)));

            ByteBuffer response = readFrame(socket.getInputStream());
            assertEquals(Protocol.ERROR_RESPONSE, response.get());
            byte[] error = new byte[response.getShort()];
            response.get(error);
            assertEquals("Topic already exists", new String(error, StandardCharsets.UTF_8));
        }
    }

    /** Broker 2 then starts without the topic, so only the controller can reject it. */
    private void forgetTopicInZookeeper(String topic) throws Exception {
        ZookeeperClient zk = new ZookeeperClient("localhost", cluster.zkPort());
        zk.connect();
        try {
            for (String partition : zk.getChildren("/topics/" + topic + "/partitions")) {
                zk.deleteNode("/topics/" + topic + "/partitions/" + partition);
            }
            zk.deleteNode("/topics/" + topic + "/partitions");
            zk.deleteNode("/topics/" + topic);
        } finally {
            zk.close();
        }
    }

    private void createTopic(SimpleKafkaClient client, String topic, int partitions) throws IOException {
        assertTrue(client.createTopic(topic, partitions, (short) 1), "create " + topic);
        cluster.awaitTopicEverywhere(topic);
    }

    private Socket rawSocket(int brokerId) throws IOException {
        Socket socket = new Socket("localhost", cluster.port(brokerId));
        socket.setTcpNoDelay(true);
        socket.setSoTimeout(5000);
        return socket;
    }

    private static byte[] frame(ByteBuffer body) {
        return ByteBuffer.allocate(4 + body.remaining()).putInt(body.remaining()).put(body).array();
    }

    private static ByteBuffer readFrame(InputStream raw) throws IOException {
        DataInputStream in = new DataInputStream(raw);
        int size = in.readInt();
        assertTrue(size >= 1 && size <= MAX_FRAME_BYTES,
                "response starts with a size prefix, got 0x" + Integer.toHexString(size));
        byte[] body = new byte[size];
        in.readFully(body);
        return ByteBuffer.wrap(body);
    }

    private static byte[] payload(int size, char fill) {
        byte[] bytes = new byte[size];
        Arrays.fill(bytes, (byte) fill);
        return bytes;
    }
}
