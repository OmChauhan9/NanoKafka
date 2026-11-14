package com.simplekafka.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PartitionTest {

    @TempDir
    Path dir;

    @Test
    void appendedMessagesSurviveAReopen() {
        Partition partition = new Partition(0, 1, List.of(), dir.toString());
        assertEquals(0, partition.append(bytes("a")));
        assertEquals(1, partition.append(bytes("b")));
        partition.close();

        Partition reopened = new Partition(0, 1, List.of(), dir.toString());
        assertEquals(2, reopened.getLogEndOffset());
        assertEquals(2, reopened.append(bytes("c")));
        assertEquals(List.of("a", "b", "c"), strings(reopened.readMessages(0, 1024)));
        reopened.close();
    }

    @Test
    void readContinuesIntoTheNextSegment() {
        Partition partition = new Partition(0, 1, List.of(), dir.toString());
        for (int i = 0; i < 13; i++) {
            partition.append(sized("m" + i, 100_000));
        }

        assertEquals(List.of("m9", "m10", "m11", "m12"), labels(partition.readMessages(9, 1024 * 1024)));
        partition.close();
    }

    @Test
    void readReturnsTheFirstMessageEvenWhenItExceedsMaxBytes() {
        Partition partition = new Partition(0, 1, List.of(), dir.toString());
        for (int i = 0; i < 3; i++) {
            partition.append(sized("m" + i, 1000));
        }

        assertEquals(List.of("m0"), labels(partition.readMessages(0, 500)));
        partition.close();
    }

    @Test
    void readCountsTheLengthHeaderAgainstMaxBytes() {
        Partition partition = new Partition(0, 1, List.of(), dir.toString());
        for (int i = 0; i < 3; i++) {
            partition.append(sized("m" + i, 1000));
        }

        assertEquals(List.of("m0", "m1"), labels(partition.readMessages(0, 2008)));
        assertEquals(List.of("m0"), labels(partition.readMessages(0, 2007)));
        partition.close();
    }

    static byte[] sized(String label, int size) {
        StringBuilder s = new StringBuilder(label).append(':');
        while (s.length() < size) {
            s.append('x');
        }
        return bytes(s.toString());
    }

    static List<String> labels(List<byte[]> messages) {
        return strings(messages).stream().map(m -> m.substring(0, m.indexOf(':'))).collect(Collectors.toList());
    }

    static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    static List<String> strings(List<byte[]> messages) {
        return messages.stream().map(m -> new String(m, StandardCharsets.UTF_8)).collect(Collectors.toList());
    }
}
