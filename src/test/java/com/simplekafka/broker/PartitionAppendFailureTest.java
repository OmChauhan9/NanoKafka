package com.simplekafka.broker;

import static com.simplekafka.broker.PartitionTest.bytes;
import static com.simplekafka.broker.PartitionTest.strings;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PartitionAppendFailureTest {

    @TempDir
    Path dir;

    private volatile Fault fault;

    @Test
    void anAppendFinishesALogWriteThatLandsAFewBytesAtATime() {
        assertShortWritesStillLandTheRecord(".log");
    }

    @Test
    void anAppendFinishesAnIndexWriteThatLandsAFewBytesAtATime() {
        assertShortWritesStillLandTheRecord(".index");
    }

    @Test
    void aLogWriteThatFailsPartwayLeavesNoTrace() {
        assertAFailedAppendLeavesNoTrace(new Fault(".log", 10, Integer.MAX_VALUE, false));
    }

    @Test
    void aFailedLogForceLeavesNoTrace() {
        assertAFailedAppendLeavesNoTrace(new Fault(".log", Long.MAX_VALUE, Integer.MAX_VALUE, true));
    }

    @Test
    void anIndexWriteThatFailsPartwayLeavesNoTrace() {
        assertAFailedAppendLeavesNoTrace(new Fault(".index", 8, Integer.MAX_VALUE, false));
    }

    @Test
    void aFailedIndexForceLeavesNoTrace() {
        assertAFailedAppendLeavesNoTrace(new Fault(".index", Long.MAX_VALUE, Integer.MAX_VALUE, true));
    }

    private void assertShortWritesStillLandTheRecord(String suffix) {
        fault = new Fault(suffix, Long.MAX_VALUE, 3, false);
        Partition partition = new Partition(0, 1, List.of(), dir.toString(), this::open);
        String a = "a".repeat(100);
        String b = "b".repeat(100);
        assertEquals(0, partition.append(bytes(a)));
        assertEquals(1, partition.append(bytes(b)));
        assertEquals(List.of(a, b), strings(partition.readMessages(0, 1024)), "read from offset 0");
        assertEquals(List.of(b), strings(partition.readMessages(1, 1024)), "read from offset 1");
        partition.close();

        Partition reopened = new Partition(0, 1, List.of(), dir.toString());
        assertEquals(2, reopened.getLogEndOffset());
        assertEquals(List.of(a, b), strings(reopened.readMessages(0, 1024)));
        reopened.close();
    }

    private void assertAFailedAppendLeavesNoTrace(Fault failure) {
        Partition partition = new Partition(0, 1, List.of(), dir.toString(), this::open);
        assertEquals(0, partition.append(bytes("a")));

        long rejected;
        try {
            fault = failure;
            rejected = partition.append(bytes("b".repeat(100)));
        } finally {
            fault = null;
        }
        assertEquals(-1, rejected);

        assertEquals(1, partition.append(bytes("c")));
        assertEquals(2, partition.append(bytes("d")));
        List<String> expected = List.of("a", "c", "d");
        for (int offset = 0; offset < expected.size(); offset++) {
            assertEquals(expected.subList(offset, expected.size()), strings(partition.readMessages(offset, 1024)),
                    "read from offset " + offset);
        }
        partition.close();

        Partition reopened = new Partition(0, 1, List.of(), dir.toString());
        assertEquals(3, reopened.getLogEndOffset());
        assertEquals(expected, strings(reopened.readMessages(0, 1024)));
        reopened.close();
    }

    private FileChannel open(Path path) throws IOException {
        FileChannel real = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE,
                StandardOpenOption.CREATE);
        return new FaultyChannel(real, path.getFileName().toString());
    }

    /**
     * How the file whose name ends in {@code suffix} misbehaves: each write lands at most
     * {@code maxBytesPerWrite} bytes, a write once {@code writeBudget} bytes have landed throws
     * like a full disk, and {@code failForce} makes force throw.
     */
    private static final class Fault {
        final String suffix;
        long writeBudget;
        final int maxBytesPerWrite;
        final boolean failForce;

        Fault(String suffix, long writeBudget, int maxBytesPerWrite, boolean failForce) {
            this.suffix = suffix;
            this.writeBudget = writeBudget;
            this.maxBytesPerWrite = maxBytesPerWrite;
            this.failForce = failForce;
        }
    }

    private final class FaultyChannel extends FileChannel {
        private final FileChannel real;
        private final String name;

        FaultyChannel(FileChannel real, String name) {
            this.real = real;
            this.name = name;
        }

        private Fault active() {
            Fault f = fault;
            return f != null && name.endsWith(f.suffix) ? f : null;
        }

        @Override
        public int write(ByteBuffer src) throws IOException {
            Fault f = active();
            if (f == null) {
                return real.write(src);
            }
            if (f.writeBudget == 0 && src.hasRemaining()) {
                throw new IOException("File too large");
            }
            ByteBuffer part = src.duplicate();
            part.limit(part.position() + (int) Math.min(src.remaining(), Math.min(f.maxBytesPerWrite, f.writeBudget)));
            int written = real.write(part);
            src.position(src.position() + written);
            f.writeBudget -= written;
            return written;
        }

        @Override
        public void force(boolean metaData) throws IOException {
            Fault f = active();
            if (f != null && f.failForce) {
                throw new IOException("force failed");
            }
            real.force(metaData);
        }

        @Override
        public int read(ByteBuffer dst) throws IOException {
            return real.read(dst);
        }

        @Override
        public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
            return real.read(dsts, offset, length);
        }

        @Override
        public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
            return real.write(srcs, offset, length);
        }

        @Override
        public long position() throws IOException {
            return real.position();
        }

        @Override
        public FileChannel position(long newPosition) throws IOException {
            real.position(newPosition);
            return this;
        }

        @Override
        public long size() throws IOException {
            return real.size();
        }

        @Override
        public FileChannel truncate(long size) throws IOException {
            real.truncate(size);
            return this;
        }

        @Override
        public long transferTo(long position, long count, WritableByteChannel target) throws IOException {
            return real.transferTo(position, count, target);
        }

        @Override
        public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException {
            return real.transferFrom(src, position, count);
        }

        @Override
        public int read(ByteBuffer dst, long position) throws IOException {
            return real.read(dst, position);
        }

        @Override
        public int write(ByteBuffer src, long position) throws IOException {
            return real.write(src, position);
        }

        @Override
        public MappedByteBuffer map(MapMode mode, long position, long size) throws IOException {
            return real.map(mode, position, size);
        }

        @Override
        public FileLock lock(long position, long size, boolean shared) throws IOException {
            return real.lock(position, size, shared);
        }

        @Override
        public FileLock tryLock(long position, long size, boolean shared) throws IOException {
            return real.tryLock(position, size, shared);
        }

        @Override
        protected void implCloseChannel() throws IOException {
            real.close();
        }
    }
}
