package com.simplekafka.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
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
    void reopenDropsATornTrailingRecord() throws IOException {
        Partition partition = new Partition(0, 1, List.of(), dir.toString());
        partition.append(bytes("a".repeat(100)));
        partition.append(bytes("b".repeat(100)));
        partition.append(bytes("c".repeat(100)));
        partition.close();
        truncateBy(dir.resolve(segmentFile(0, ".log")), 50);

        Partition reopened = new Partition(0, 1, List.of(), dir.toString());
        assertEquals(2, reopened.getLogEndOffset());
        assertEquals(2, reopened.append(bytes("d")));
        assertEquals(List.of("a".repeat(100), "b".repeat(100), "d"), strings(reopened.readMessages(0, 1024)));
        reopened.close();
    }

    @Test
    void reopenRebuildsAnIndexShorterThanTheLog() throws IOException {
        Partition partition = new Partition(0, 1, List.of(), dir.toString());
        partition.append(bytes("a"));
        partition.append(bytes("b"));
        partition.append(bytes("c"));
        partition.close();
        truncateBy(dir.resolve(segmentFile(0, ".index")), 16);

        Partition reopened = new Partition(0, 1, List.of(), dir.toString());
        assertEquals(List.of("c"), strings(reopened.readMessages(2, 1024)));
        assertEquals(3, reopened.append(bytes("d")));
        assertEquals(List.of("a", "b", "c", "d"), strings(reopened.readMessages(0, 1024)));
        reopened.close();
    }

    @Test
    void reopenRebuildsAMissingActiveIndex() throws IOException {
        Partition partition = new Partition(0, 1, List.of(), dir.toString());
        partition.append(bytes("a"));
        partition.append(bytes("b"));
        partition.close();
        Files.delete(dir.resolve(segmentFile(0, ".index")));

        Partition reopened = new Partition(0, 1, List.of(), dir.toString());
        assertEquals(2, reopened.getLogEndOffset());
        assertEquals(2, reopened.append(bytes("c")));
        assertEquals(List.of("a", "b", "c"), strings(reopened.readMessages(0, 1024)));
        reopened.close();
    }

    @Test
    void reopenRebuildsAMissingIndexOfAnOlderSegment() throws IOException {
        Partition partition = new Partition(0, 1, List.of(), dir.toString());
        for (int i = 0; i < 13; i++) {
            partition.append(bytes(i + ":" + "x".repeat(100_000)));
        }
        partition.close();
        Files.delete(dir.resolve(segmentFile(0, ".index")));

        Partition reopened = new Partition(0, 1, List.of(), dir.toString());
        List<String> heads = strings(reopened.readMessages(0, 300_000)).stream()
                .map(m -> m.substring(0, m.indexOf(':')))
                .collect(Collectors.toList());
        assertEquals(List.of("0", "1"), heads);
        assertEquals(13, reopened.getLogEndOffset());
        reopened.close();
    }

    static String segmentFile(long baseOffset, String suffix) {
        return String.format("%020d", baseOffset) + suffix;
    }

    static void truncateBy(Path file, int bytes) throws IOException {
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.WRITE)) {
            channel.truncate(channel.size() - bytes);
        }
    }

    @Test
    void readContinuesIntoTheNextSegment() {
        Partition partition = new Partition(0, 1, List.of(), dir.toString());
        for (int i = 0; i < 13; i++) {
            partition.append(sized("m" + i, 100_000));
        }

        List<String> labels = strings(partition.readMessages(9, 1024 * 1024)).stream()
                .map(m -> m.substring(0, m.indexOf(':')))
                .collect(Collectors.toList());
        assertEquals(List.of("m9", "m10", "m11", "m12"), labels);
        partition.close();
    }

    static byte[] sized(String label, int size) {
        StringBuilder s = new StringBuilder(label).append(':');
        while (s.length() < size) {
            s.append('x');
        }
        return bytes(s.toString());
    }

    static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    static List<String> strings(List<byte[]> messages) {
        return messages.stream().map(m -> new String(m, StandardCharsets.UTF_8)).collect(Collectors.toList());
    }
}
