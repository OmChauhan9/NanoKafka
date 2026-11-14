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

    static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    static List<String> strings(List<byte[]> messages) {
        return messages.stream().map(m -> new String(m, StandardCharsets.UTF_8)).collect(Collectors.toList());
    }
}
