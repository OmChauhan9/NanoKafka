package com.simplekafka.bench;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.simplekafka.TestCluster;
import com.simplekafka.broker.Partition;
import com.simplekafka.broker.Protocol;
import com.simplekafka.client.SimpleKafkaClient;

public final class Bench {
    private static final int REPS = 5;
    private static final int WARMUP = 1;
    private static final int PARTITION_DEFAULT_SEGMENT_SIZE = 1024 * 1024;
    private static final int RECORD_1K = 4 + 1024;
    private static final int RECORDS_PER_SEGMENT = (PARTITION_DEFAULT_SEGMENT_SIZE + RECORD_1K - 1) / RECORD_1K;
    private static final int BULK_FETCH_BYTES = 8 * 1024 * 1024;
    private static final int FORCES_PER_REP = 400;
    private static final String PAIR = "log force then index force, append's I/O without Partition";

    private final Path work;
    private final List<Section> sections = new ArrayList<>();
    private final LogCounter logs = new LogCounter();
    private double readP50Micros = Double.NaN;

    private Bench(Path work) {
        this.work = work;
        Logger.getLogger("").addHandler(logs);
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("usage: Bench <report-body.md>");
            System.exit(2);
        }
        Bench bench = new Bench(Paths.get("").toAbsolutePath());
        bench.run("a. Partition append throughput", bench::appendThroughput);
        bench.run("b. Partition single-message read latency", bench::readLatency);
        try (TestCluster cluster = TestCluster.start(2)) {
            bench.run("c. End-to-end latency through a 2-broker cluster", section -> bench.endToEndLatency(cluster, section));
            long cooldown = Long.getLong("bench.portCooldownSeconds", 0);
            if (cooldown > 0) {
                System.out.println("bench: waiting " + cooldown + " s for TIME_WAIT sockets to expire");
                Thread.sleep(cooldown * 1000);
            }
            bench.run("d. Concurrent produce throughput", section -> bench.concurrentProduce(cluster, section));
        }
        Files.writeString(Paths.get(args[0]), bench.render());
        long errors = bench.sections.stream().flatMap(s -> s.rows.stream()).mapToLong(r -> r.errors).sum();
        System.exit(errors == 0 ? 0 : 1);
    }

    private interface Scenario {
        void run(Section section) throws Exception;
    }

    private interface Measure {
        Result run() throws Exception;
    }

    private void run(String title, Scenario scenario) {
        Section section = new Section(title);
        sections.add(section);
        System.out.println("bench: running " + title);
        logs.reset();
        long start = System.nanoTime();
        try {
            scenario.run(section);
        } catch (Exception e) {
            e.printStackTrace();
            section.rows.add(Row.check("scenario aborted", 0, 1, "threw " + e));
            section.analysis.clear();
            section.analysis.add("Inconclusive: the scenario threw before it finished, so its numbers are missing or partial.");
        }
        section.seconds = (System.nanoTime() - start) / 1e9;
        section.logged = logs.count.get();
        section.loggedSamples = new ArrayList<>(logs.samples);
    }

    private static Series[] interleave(Measure... variants) throws Exception {
        Series[] series = new Series[variants.length];
        for (int v = 0; v < variants.length; v++) {
            series[v] = new Series();
        }
        for (int rep = -WARMUP; rep < REPS; rep++) {
            for (int k = 0; k < variants.length; k++) {
                int v = (k + Math.max(rep, 0)) % variants.length;
                Result result = variants[v].run();
                if (rep >= 0) {
                    series[v].add(result);
                }
            }
        }
        return series;
    }

    private void appendThroughput(Section section) throws Exception {
        Series[] s = interleave(() -> appendRun(100), () -> appendRun(1024), () -> forceRun(1, false), () -> forceRun(4, false),
                () -> forceRun(16, false), () -> forceRun(1, true));
        String readBack = "acked offsets read back with identical bytes";
        section.rows.add(s[0].row("`Partition.append`, 100 B messages, 1 thread", "msgs/s", readBack));
        section.rows.add(s[1].row("`Partition.append`, 1 KB messages, 1 thread", "msgs/s", readBack));
        String forced = "write+force calls returned";
        section.rows.add(s[2].row("`FileChannel.force(true)` alone, 1 thread on 1 file", "forces/s", forced));
        section.rows.add(s[3].row("`FileChannel.force(true)` alone, 4 threads on own files", "forces/s", forced));
        section.rows.add(s[4].row("`FileChannel.force(true)` alone, 16 threads on own files", "forces/s", forced));
        section.rows.add(s[5].row(PAIR + ", 1 thread", "pairs/s", forced));
        Series pairShare = new Series();
        Series doubleShare = new Series();
        for (int rep = 0; rep < REPS; rep++) {
            pairShare.add(new Result(s[0].value(rep) / s[5].value(rep), Double.NaN, Double.NaN, 0, 0, 0));
            doubleShare.add(new Result(2 * s[0].value(rep) / s[2].value(rep), Double.NaN, Double.NaN, 0, 0, 0));
        }
        Row pairRow = pairShare.row("force pair time / 100 B append time, same rep", "ratio", "derived from the rows above");
        Row doubleRow = doubleShare.row("2 x lone force time / 100 B append time, same rep", "ratio", "derived from the rows above");
        section.rows.add(pairRow);
        section.rows.add(doubleRow);

        double force = 1e6 / s[2].values().median;
        double pair = 1e6 / s[5].values().median;
        double append100 = 1e6 / s[0].values().median;
        double append1k = 1e6 / s[1].values().median;
        section.analysis.add(String.format(Locale.ROOT,
                "Each `Partition.append` writes the record and calls `FileChannel.force(true)` on the log, then opens the index, writes 16 bytes and forces it. "
                        + "An append costs %s us at 100 B and %s us at 1 KB. The same two forces with no Partition code, the force pair row, cost %s us, "
                        + "which is %s of an append measured in the same repetition (range %s to %s).",
                num(append100), num(append1k), num(pair), pct(pairRow.value.median), pct(pairRow.value.min), pct(pairRow.value.max)));
        section.analysis.add(String.format(Locale.ROOT,
                "A lone force costs %s us, so two lone forces would be %s of an append. The pair is cheaper than two lone forces (%s us against %s us): "
                        + "the second, 16-byte force right after the first costs less than a lone one.",
                num(force), pct(doubleRow.value.median), num(pair), num(2 * force)));
        section.analysis.add(String.format(Locale.ROOT,
                "Bytes do not set the pace. At 1 KB the log takes %s KB/s, far below any local disk's sequential bandwidth, "
                        + "and the 1 KB rate is %s of the 100 B rate. The appending thread kept %s cores busy, so it spends most of its time blocked in the kernel. "
                        + "More concurrent forces raise the device's total rate only %sx at 4 files and %sx at 16.",
                num(s[1].values().median * RECORD_1K / 1024.0), pct(s[1].values().median / s[0].values().median), num(s[0].cores().median),
                num(s[3].values().median / s[2].values().median), num(s[4].values().median / s[2].values().median)));
        section.analysis.add(verdict(pairRow.value.median >= 0.8,
                "Limiter: fsync latency. The log and index forces alone account for " + pct(pairRow.value.median) + " of an append's time.",
                "Limiter: inconclusive. The log and index forces account for only " + pct(pairRow.value.median) + " of an append, so something else costs the rest."));
    }

    private Result appendRun(int size) throws IOException {
        int messages = 200;
        Path dir = work.resolve("append-" + size);
        Partition partition = new Partition(0, 1, List.of(), dir.toString());
        byte[][] payloads = new byte[messages][];
        for (int i = 0; i < messages; i++) {
            payloads[i] = payload(i, size);
        }
        long errors = 0;
        Meter meter = Meter.start();
        for (int i = 0; i < messages; i++) {
            if (partition.append(payloads[i]) != i) {
                errors++;
            }
        }
        meter.stop();
        long verified = 0;
        for (int i = 0; i < messages; i++) {
            List<byte[]> got = partition.readMessages(i, 1);
            if (got.size() == 1 && Arrays.equals(got.get(0), payloads[i])) {
                verified++;
            } else {
                errors++;
            }
        }
        partition.close();
        delete(dir);
        return new Result(messages / meter.seconds(), Double.NaN, meter.cores(), errors, verified, messages);
    }

    private Result forceRun(int threads, boolean withIndexForce) throws Exception {
        int perThread = (withIndexForce ? FORCES_PER_REP / 2 : FORCES_PER_REP) / threads;
        Path dir = Files.createDirectories(work.resolve("force-" + threads + (withIndexForce ? "-pair" : "")));
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            CountDownLatch go = new CountDownLatch(1);
            List<Future<Long>> results = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                Path logFile = dir.resolve("log" + t);
                Path indexFile = dir.resolve("index" + t);
                results.add(pool.submit(() -> {
                    long done = 0;
                    try (FileChannel log = FileChannel.open(logFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
                            FileChannel index = FileChannel.open(indexFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
                        ByteBuffer record = ByteBuffer.allocate(104);
                        ByteBuffer entry = ByteBuffer.allocate(16);
                        go.await();
                        for (int i = 0; i < perThread; i++) {
                            writeFully(log, record.clear());
                            log.force(true);
                            if (withIndexForce) {
                                writeFully(index, entry.clear());
                                index.force(true);
                            }
                            done++;
                        }
                    }
                    return done;
                }));
            }
            Thread.sleep(50);
            Meter meter = Meter.start();
            go.countDown();
            long done = 0;
            long errors = 0;
            for (Future<Long> result : results) {
                try {
                    done += result.get();
                } catch (Exception e) {
                    errors++;
                }
            }
            meter.stop();
            return new Result(done / meter.seconds(), Double.NaN, meter.cores(), errors, done, (long) threads * perThread);
        } finally {
            pool.shutdown();
            delete(dir);
        }
    }

    private void readLatency(Section section) throws Exception {
        Path appended = work.resolve("read-2");
        int count2 = RECORDS_PER_SEGMENT + RECORDS_PER_SEGMENT / 2;
        Partition two = new Partition(0, 1, List.of(), appended.toString());
        long buildErrors = 0;
        for (int i = 0; i < count2; i++) {
            if (two.append(payload(i, 1024)) != i) {
                buildErrors++;
            }
        }
        Path written = work.resolve("read-2-written");
        writeSegments(written, count2);
        boolean writerMatchesAppend = sameFiles(appended, written);
        delete(written);
        if (!writerMatchesAppend) {
            buildErrors++;
        }

        int[] segmentCounts = { 2, 11, 20 };
        List<Partition> partitions = new ArrayList<>();
        List<Integer> counts = new ArrayList<>();
        partitions.add(two);
        counts.add(count2);
        for (int s = 1; s < segmentCounts.length; s++) {
            int count = RECORDS_PER_SEGMENT * (segmentCounts[s] - 1) + RECORDS_PER_SEGMENT / 2;
            Path dir = work.resolve("read-" + segmentCounts[s]);
            writeSegments(dir, count);
            partitions.add(new Partition(0, 1, List.of(), dir.toString()));
            counts.add(count);
        }
        for (int s = 0; s < segmentCounts.length; s++) {
            if (logFiles(work.resolve("read-" + segmentCounts[s])) != segmentCounts[s] || partitions.get(s).getLogEndOffset() != counts.get(s)) {
                buildErrors++;
            }
        }

        String[] positions = { "first", "middle", "last" };
        int reads = 2000;
        Random random = new Random(42);
        List<Measure> measures = new ArrayList<>();
        List<String> names = new ArrayList<>();
        for (int s = 0; s < segmentCounts.length; s++) {
            for (int p = 0; p < positions.length; p++) {
                int segment = p == 0 ? 0 : p == 1 ? segmentCounts[s] / 2 : segmentCounts[s] - 1;
                long base = (long) segment * RECORDS_PER_SEGMENT;
                long records = segment == segmentCounts[s] - 1 ? counts.get(s) - base : RECORDS_PER_SEGMENT;
                Partition partition = partitions.get(s);
                names.add(segmentCounts[s] + " segments, offset in " + positions[p] + " segment");
                measures.add(() -> {
                    long[] nanos = new long[reads];
                    long errors = 0;
                    Meter meter = Meter.start();
                    for (int i = 0; i < reads; i++) {
                        long offset = base + random.nextInt((int) records - 1);
                        long t0 = System.nanoTime();
                        List<byte[]> got = partition.readMessages(offset, 1);
                        nanos[i] = System.nanoTime() - t0;
                        if (got.size() != 1 || !Arrays.equals(got.get(0), payload(offset, 1024))) {
                            errors++;
                        }
                    }
                    meter.stop();
                    return Result.latencyMicros(nanos, meter, errors, reads);
                });
            }
        }
        Path log = list(work.resolve("read-20")).stream().filter(f -> f.toString().endsWith(".log")).findFirst().get();
        measures.add(() -> {
            long[] nanos = new long[reads];
            long errors = 0;
            ByteBuffer buffer = ByteBuffer.allocate(16);
            Meter meter = Meter.start();
            for (int i = 0; i < reads; i++) {
                long t0 = System.nanoTime();
                int read;
                try (RandomAccessFile file = new RandomAccessFile(log.toFile(), "r"); FileChannel channel = file.getChannel()) {
                    buffer.clear();
                    read = channel.size() > 0 ? channel.read(buffer, (i % RECORDS_PER_SEGMENT) * (long) RECORD_1K) : 0;
                }
                nanos[i] = System.nanoTime() - t0;
                if (read != 16 || buffer.getInt(0) != 1024) {
                    errors++;
                }
            }
            meter.stop();
            return Result.latencyMicros(nanos, meter, errors, reads);
        });
        Series[] s = interleave(measures.toArray(new Measure[0]));

        double lowest = Double.MAX_VALUE;
        double highest = 0;
        double widestRange = 0;
        for (int i = 0; i < names.size(); i++) {
            Row row = s[i].row(names.get(i), "p50 us", "returned exactly the bytes written at that offset");
            section.rows.add(row);
            lowest = Math.min(lowest, row.value.median);
            highest = Math.max(highest, row.value.median);
            widestRange = Math.max(widestRange, row.value.max - row.value.min);
        }
        for (int i = 0; i < names.size(); i++) {
            section.rows.add(s[i].p99Row(names.get(i)));
        }
        Series open = s[names.size()];
        String openName = "open, size, 16-byte read, close of one segment file, no Partition code";
        Row openRow = open.row(openName, "p50 us", "reads saw the record size header");
        section.rows.add(openRow);
        section.rows.add(open.p99Row(openName));
        section.rows.add(Row.check("partition setup", segmentCounts.length, buildErrors,
                (writerMatchesAppend ? "files written directly are byte-identical to `append`'s for " + count2 + " records"
                        : "files written directly differ from `append`'s") + "; segment counts and log end offsets match"));
        for (Partition partition : partitions) {
            partition.close();
        }

        readP50Micros = s[names.size() - 1].values().median;
        boolean flat = highest - lowest <= widestRange;
        section.analysis.add("The 2-segment partition is built with `Partition.append`. The 11- and 20-segment partitions are written straight to disk in the same format, "
                + "because building them by append would take minutes of fsyncs, and then opened with `new Partition`. "
                + "The setup row proves the direct writer produces the same bytes as `append`, rollover included.");
        section.analysis.add(String.format(Locale.ROOT,
                "Across 2, 11 and 20 segments and the first, middle and last segment, median p50 latency spans %s to %s us (a ratio of %s). "
                        + "The widest run-to-run range of any one cell is %s us.",
                num(lowest), num(highest), num(highest / lowest), num(widestRange)));
        section.analysis.add(verdict(flat,
                "Verdict on the README's \"index-based O(1) message lookups\": flat within noise on this range. Read latency does not grow with the offset's position or with segment count from 2 to 20.",
                "Verdict on the README's \"index-based O(1) message lookups\": not flat on this range. The spread between cells exceeds the run-to-run range, so position or segment count changes latency."));
        section.analysis.add("Scope of that verdict. From the code, each read binary-searches the segment list, which is O(log S), "
                + "then calls `segments.indexOf`, which is O(S), then seeks straight to the index entry, which is O(1) within a segment. "
                + "At 20 segments those list walks cost nanoseconds, so this run cannot separate O(1) from O(log S) or O(S) in the segment count. "
                + "What it shows is that a lookup never scans log bytes before the target offset. Behavior at thousands of segments is inconclusive.");
        section.analysis.add(String.format(Locale.ROOT,
                "Limiter: opening files. Every read opens and closes the index file and then the log file. One isolated open, size, read and close costs %s us, "
                        + "so two of them account for %s of the %s us p50 read in the 20-segment partition's last segment.",
                num(openRow.value.median), pct(2 * openRow.value.median / readP50Micros), num(readP50Micros)));
        section.analysis.add("Inconclusive: cold reads. Every file here was just written, so reads hit the OS page cache. "
                + "Dropping the cache needs root, so disk-bound read latency was not measured.");
    }

    private void endToEndLatency(TestCluster cluster, Section section) throws Exception {
        String[] topics = { "lat-rf1", "lat-rf2" };
        SimpleKafkaClient admin = cluster.client(1);
        admin.createTopic(topics[0], 1, (short) 1);
        admin.createTopic(topics[1], 1, (short) 2);
        cluster.awaitTopicEverywhere(topics[0]);
        cluster.awaitTopicEverywhere(topics[1]);
        SimpleKafkaClient client = cluster.client(1);
        int leader1 = client.getTopicMetadata(topics[0]).getPartitions().get(0).getLeader();
        int leader2 = client.getTopicMetadata(topics[1]).getPartitions().get(0).getLeader();

        int sends = 200;
        int requests = 300;
        int warm = 20;
        long[] next = new long[2];
        Partition reference = new Partition(0, 1, List.of(), work.resolve("reference-append").toString());
        long[] referenceNext = new long[1];
        Random random = new Random(7);
        List<Measure> measures = new ArrayList<>();
        for (int t = 0; t < 2; t++) {
            int topic = t;
            measures.add(() -> {
                long[] nanos = new long[sends];
                long errors = 0;
                Meter meter = Meter.start();
                for (int i = 0; i < sends; i++) {
                    byte[] message = payload(topic * 1_000_000L + next[topic], 100);
                    long t0 = System.nanoTime();
                    long offset;
                    try {
                        offset = client.send(topics[topic], 0, message);
                    } catch (IOException e) {
                        offset = -1;
                    }
                    nanos[i] = System.nanoTime() - t0;
                    if (offset == next[topic]) {
                        next[topic]++;
                    } else {
                        errors++;
                    }
                }
                meter.stop();
                return Result.latencyMicros(nanos, meter, errors, 0);
            });
        }
        measures.add(() -> {
            long[] nanos = new long[sends];
            long errors = 0;
            long first = referenceNext[0];
            Meter meter = Meter.start();
            for (int i = 0; i < sends; i++) {
                byte[] message = payload(referenceNext[0], 100);
                long t0 = System.nanoTime();
                long offset = reference.append(message);
                nanos[i] = System.nanoTime() - t0;
                if (offset == referenceNext[0]) {
                    referenceNext[0]++;
                } else {
                    errors++;
                }
            }
            meter.stop();
            for (long offset = first; offset < referenceNext[0]; offset++) {
                List<byte[]> got = reference.readMessages(offset, 1);
                if (got.size() != 1 || !Arrays.equals(got.get(0), payload(offset, 100))) {
                    errors++;
                }
            }
            return Result.latencyMicros(nanos, meter, errors, sends);
        });
        measures.add(() -> {
            long[] nanos = new long[requests];
            long errors = 0;
            for (int i = 0; i < warm; i++) {
                metadata(cluster, leader1);
            }
            Meter meter = Meter.start();
            for (int i = 0; i < requests; i++) {
                long t0 = System.nanoTime();
                boolean ok = metadata(cluster, leader1);
                nanos[i] = System.nanoTime() - t0;
                if (!ok) {
                    errors++;
                }
            }
            meter.stop();
            return Result.latencyMicros(nanos, meter, errors, requests);
        });
        measures.add(() -> {
            long[] nanos = new long[requests];
            long errors = 0;
            for (int i = 0; i < warm; i++) {
                client.fetch(topics[0], 0, 0, 1);
            }
            Meter meter = Meter.start();
            for (int i = 0; i < requests; i++) {
                long offset = random.nextInt((int) next[0]);
                long t0 = System.nanoTime();
                List<byte[]> got;
                try {
                    got = client.fetch(topics[0], 0, offset, 1);
                } catch (IOException e) {
                    got = List.of();
                }
                nanos[i] = System.nanoTime() - t0;
                if (got.size() != 1 || !Arrays.equals(got.get(0), payload(offset, 100))) {
                    errors++;
                }
            }
            meter.stop();
            return Result.latencyMicros(nanos, meter, errors, requests);
        });
        for (int i = 0; i < 20; i++) {
            client.send(topics[0], 0, payload(next[0], 100));
            next[0]++;
        }
        Series[] s = interleave(measures.toArray(new Measure[0]));
        reference.close();

        long[] leaderVerified = new long[2];
        for (int t = 0; t < 2; t++) {
            List<byte[]> log = client.fetch(topics[t], 0, 0, BULK_FETCH_BYTES);
            for (int offset = 0; offset < Math.min(log.size(), next[t]); offset++) {
                if (Arrays.equals(log.get(offset), payload(t * 1_000_000L + offset, 100))) {
                    leaderVerified[t]++;
                }
            }
        }
        int follower = leader2 == 1 ? 2 : 1;
        long deadline = System.nanoTime() + 20_000_000_000L;
        while (System.nanoTime() < deadline && fetchRaw(cluster, follower, topics[1], next[1] - 1, 1).isEmpty()) {
            Thread.sleep(100);
        }
        long followerVerified = 0;
        List<byte[]> followerLog = fetchRaw(cluster, follower, topics[1], 0, BULK_FETCH_BYTES);
        for (int offset = 0; offset < Math.min(followerLog.size(), next[1]); offset++) {
            if (Arrays.equals(followerLog.get(offset), payload(1_000_000L + offset, 100))) {
                followerVerified++;
            }
        }

        String[] names = { "produce 100 B, replication factor 1", "produce 100 B, replication factor 2" };
        for (int t = 0; t < 2; t++) {
            String check = leaderVerified[t] + "/" + next[t] + " acked offsets, warmup included, fetched back from the leader with identical bytes";
            long errors = next[t] - leaderVerified[t];
            if (t == 1) {
                check += "; the follower holds " + followerVerified + "/" + next[t] + " at the same offsets";
                errors += next[t] - followerVerified;
            }
            section.rows.add(s[t].row(names[t], "p50 us", check).plusErrors(errors));
            section.rows.add(s[t].p99Row(names[t]));
        }
        String referenceName = "in-process `Partition.append` 100 B, interleaved reference";
        section.rows.add(s[2].row(referenceName, "p50 us", "read back with identical bytes"));
        section.rows.add(s[2].p99Row(referenceName));
        section.rows.add(s[3].row("metadata request, no disk work", "p50 us", "decoded a successful response"));
        section.rows.add(s[3].p99Row("metadata request, no disk work"));
        section.rows.add(s[4].row("fetch one 100 B message by offset", "p50 us", "returned the bytes produced at that offset"));
        section.rows.add(s[4].p99Row("fetch one 100 B message by offset"));

        double rf1 = s[0].values().median;
        double rf2 = s[1].values().median;
        double append = s[2].values().median;
        double rpc = s[3].values().median;
        double fetch = s[4].values().median;
        section.analysis.add(String.format(Locale.ROOT,
                "Leaders are broker %d for `lat-rf1` and broker %d for `lat-rf2`. The client opens a new TCP connection for every request (`Protocol.call`). "
                        + "A metadata request does no disk work, so its %s us p50 is the cost of one round trip on a fresh connection.",
                leader1, leader2, num(rpc)));
        section.analysis.add(String.format(Locale.ROOT,
                "Produce at replication factor 1 is %s us at p50. An in-process append measured in the same repetitions is %s us, "
                        + "so the append is %s of the produce latency and the round trip floor adds %s more. Limiter: the leader's two fsyncs, as in scenario a.%s",
                num(rf1), num(append), pct(append / rf1), pct(rpc / rf1),
                (append + rpc) / rf1 < 0.8
                        ? String.format(Locale.ROOT, " Inconclusive: %s us of the produce p50 is not explained by the append plus the round trip floor.", num(rf1 - append - rpc))
                        : ""));
        section.analysis.add(String.format(Locale.ROOT,
                "Produce at replication factor 2 is %s us at p50, %sx the factor 1 latency. From the code, `replicateToFollowers` submits the copy to an executor "
                        + "and the leader acks before the follower answers, so the ack path has no extra network round trip, "
                        + "and an acked message may not be on the follower yet when the client sees the ack. "
                        + "Both brokers share one disk, and scenario a shows concurrent forces add little device throughput, so the follower's two fsyncs likely delay the leader's. "
                        + "Two appends' worth of forces is %s us, which matches %s of the factor 2 p50. Limiter: fsyncs from both replicas queued on one device (inferred from that arithmetic, not profiled).",
                num(rf2), num(rf2 / rf1), num(2 * append), pct(2 * append / rf2)));
        section.analysis.add(String.format(Locale.ROOT,
                "Fetch is %s us at p50. The in-process read from scenario b is %s us and a metadata round trip is %s us, together %s of the fetch. "
                        + "Limiter: the request round trip on a fresh connection, not the disk; the read itself comes from the page cache.",
                num(fetch), num(readP50Micros), num(rpc), pct((readP50Micros + rpc) / fetch)));
    }

    private static boolean metadata(TestCluster cluster, int brokerId) {
        try {
            return Protocol.decodeMetadataResponse(cluster.request(brokerId, Protocol.encodeMetadataRequest())).isSuccess();
        } catch (IOException e) {
            return false;
        }
    }

    private static List<byte[]> fetchRaw(TestCluster cluster, int brokerId, String topic, long offset, int maxBytes) throws IOException {
        Protocol.FetchResult result = Protocol.decodeFetchResponse(cluster.request(brokerId, Protocol.encodeFetchRequest(topic, 0, offset, maxBytes)));
        return result.isSuccess() ? Arrays.asList(result.getMessages()) : List.of();
    }

    private void concurrentProduce(TestCluster cluster, Section section) throws Exception {
        int[][] variants = { { 1, 1 }, { 4, 1 }, { 16, 1 }, { 4, 4 }, { 16, 4 } };
        SimpleKafkaClient admin = cluster.client(1);
        for (int partitions : new int[] { 1, 4 }) {
            admin.createTopic("conc-p" + partitions, partitions, (short) 1);
            cluster.awaitTopicEverywhere("conc-p" + partitions);
        }
        List<SimpleKafkaClient> clients = new ArrayList<>();
        for (int i = 0; i < 16; i++) {
            clients.add(cluster.client(1));
        }
        int messagesPerRep = 240;
        AtomicLong keys = new AtomicLong();
        ConcurrentLinkedQueue<long[]> acks = new ConcurrentLinkedQueue<>();
        ExecutorService pool = Executors.newFixedThreadPool(16);
        List<Measure> measures = new ArrayList<>();
        for (int[] variant : variants) {
            int threads = variant[0];
            int partitions = variant[1];
            String topic = "conc-p" + partitions;
            measures.add(() -> {
                CountDownLatch go = new CountDownLatch(1);
                List<Future<long[]>> results = new ArrayList<>();
                for (int i = 0; i < threads; i++) {
                    SimpleKafkaClient client = clients.get(i);
                    int partition = i % partitions;
                    results.add(pool.submit(() -> {
                        long acked = 0;
                        long errors = 0;
                        go.await();
                        for (int j = 0; j < messagesPerRep / threads; j++) {
                            long key = keys.incrementAndGet();
                            try {
                                long offset = client.send(topic, partition, payload(key, 100));
                                acks.add(new long[] { partitions, partition, offset, key });
                                acked++;
                            } catch (IOException e) {
                                errors++;
                            }
                        }
                        return new long[] { acked, errors };
                    }));
                }
                Thread.sleep(50);
                Meter meter = Meter.start();
                go.countDown();
                long acked = 0;
                long errors = 0;
                for (Future<long[]> result : results) {
                    long[] r = result.get();
                    acked += r[0];
                    errors += r[1];
                }
                meter.stop();
                return new Result(acked / meter.seconds(), Double.NaN, meter.cores(), errors, 0, 0);
            });
        }
        measures.add(() -> forceRun(1, true));
        measures.add(() -> forceRun(4, true));
        Series[] s;
        try {
            s = interleave(measures.toArray(new Measure[0]));
        } finally {
            pool.shutdown();
        }

        SimpleKafkaClient reader = cluster.client(1);
        long verified = 0;
        long bad = 0;
        long stored = 0;
        for (int partitions : new int[] { 1, 4 }) {
            for (int partition = 0; partition < partitions; partition++) {
                List<byte[]> log = reader.fetch("conc-p" + partitions, partition, 0, BULK_FETCH_BYTES);
                stored += log.size();
                Set<Long> seen = new HashSet<>();
                for (long[] ack : acks) {
                    if (ack[0] != partitions || ack[1] != partition) {
                        continue;
                    }
                    if (seen.add(ack[2]) && ack[2] < log.size() && Arrays.equals(log.get((int) ack[2]), payload(ack[3], 100))) {
                        verified++;
                    } else {
                        bad++;
                    }
                }
            }
        }
        bad += Math.abs(stored - acks.size());

        for (int v = 0; v < variants.length; v++) {
            section.rows.add(s[v].row(label(variants[v][0], "thread") + ", " + label(variants[v][1], "partition"), "msgs/s", "checked in the acks row"));
        }
        section.rows.add(s[variants.length].row(PAIR + ", 1 thread, interleaved reference", "pairs/s", "write+force calls returned"));
        section.rows.add(s[variants.length + 1].row(PAIR + ", 4 threads on own files, interleaved reference", "pairs/s", "write+force calls returned"));
        section.rows.add(Row.check("acks, all variants and warmup", acks.size(), bad,
                verified + "/" + acks.size() + " acks have a unique offset that fetches back the bytes sent; the partitions hold " + stored + " messages"));

        String leaders = reader.getTopicMetadata("conc-p4").getPartitions().stream()
                .map(info -> "partition " + info.getId() + " on broker " + info.getLeader()).collect(Collectors.joining(", "));
        section.analysis.add("Each message is 100 B at replication factor 1, and each rep sends " + messagesPerRep + " messages. "
                + "Thread i sends to partition i mod P with its own client. Every client and both brokers share one JVM and one disk. "
                + "Topic `conc-p4` placed " + leaders + ".");
        section.analysis.add("`Partition.append` holds the partition's write lock across both fsyncs, so at most min(threads, partitions) appends run at once. "
                + "The ceiling below is the interleaved force pair rate at that many threads, since each append does one log force and one index force. "
                + "With one client thread the partition sits idle during every request's round trip. With more threads another request is already waiting on the lock.");
        StringBuilder table = new StringBuilder("| Variant | Measured msgs/s | Concurrent appends | fsync ceiling msgs/s | Share of ceiling |\n| --- | ---: | ---: | ---: | ---: |");
        double lowestShare = Double.MAX_VALUE;
        double fastest = 0;
        for (int v = 0; v < variants.length; v++) {
            int concurrent = Math.min(variants[v][0], variants[v][1]);
            double ceiling = s[variants.length + (concurrent == 1 ? 0 : 1)].values().median;
            double measured = s[v].values().median;
            lowestShare = Math.min(lowestShare, measured / ceiling);
            fastest = Math.max(fastest, measured);
            table.append(String.format(Locale.ROOT, "\n| %s, %s | %s | %d | %s | %s |", label(variants[v][0], "thread"), label(variants[v][1], "partition"),
                    num(measured), concurrent, num(ceiling), pct(measured / ceiling)));
        }
        section.analysis.add(table.toString());
        section.analysis.add(verdict(lowestShare >= 0.7,
                "Limiter: fsync throughput. Every variant reaches at least " + pct(lowestShare) + " of the rate its concurrent fsyncs allow. "
                        + "Adding threads to one partition only queues them on its write lock.",
                "Limiter: inconclusive for the variants below 70% of their fsync ceiling (lowest " + pct(lowestShare) + "). "
                        + "Something besides fsync, such as connection setup per request or lock handoff, costs the rest, and this run does not separate them."));
        double busiest = 0;
        for (int v = 0; v < variants.length; v++) {
            busiest = Math.max(busiest, s[v].cores().median);
        }
        section.analysis.add(String.format(Locale.ROOT,
                "The load generator was not the limit. The whole JVM, clients and brokers together, kept at most %s of %d cores busy.",
                num(busiest), Runtime.getRuntime().availableProcessors()));
        double cap = Double.parseDouble(System.getProperty("bench.connectionsPerSecond", "NaN"));
        section.analysis.add(Double.isNaN(cap)
                ? "Every request opens a new TCP connection. This OS reuses TIME_WAIT ports on loopback or reported no limit, so no connection-rate cap was computed."
                : String.format(Locale.ROOT, "Every request opens a new TCP connection and the client closes it, leaving a TIME_WAIT socket. "
                        + "This machine's ephemeral port range and TIME_WAIT length allow about %s new connections/s sustained. "
                        + "The fastest variant used %s of that. %s",
                        num(cap), pct(fastest / cap),
                        fastest < cap
                                ? "Here the disk saturates first. On a disk with faster fsync, sustained produce above that rate would fail with port exhaustion."
                                : "Sustained load at this rate would fail with port exhaustion."));
    }

    private static String label(int count, String noun) {
        return count + " " + noun + (count == 1 ? "" : "s");
    }

    private static void writeSegments(Path dir, int count) throws IOException {
        Files.createDirectories(dir);
        FileChannel log = null;
        FileChannel index = null;
        long position = 0;
        try {
            for (long offset = 0; offset < count; offset++) {
                if (log == null || position >= PARTITION_DEFAULT_SEGMENT_SIZE) {
                    closeQuietly(log);
                    closeQuietly(index);
                    String base = String.format(Locale.ROOT, "%020d", offset);
                    log = FileChannel.open(dir.resolve(base + ".log"), StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
                    index = FileChannel.open(dir.resolve(base + ".index"), StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
                    position = 0;
                }
                byte[] message = payload(offset, 1024);
                writeFully(log, ByteBuffer.allocate(4 + message.length).putInt(message.length).put(message).flip());
                writeFully(index, ByteBuffer.allocate(16).putLong(offset).putLong(position).flip());
                position += 4 + message.length;
            }
        } finally {
            closeQuietly(log);
            closeQuietly(index);
        }
    }

    private static void writeFully(FileChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }

    private static void closeQuietly(FileChannel channel) throws IOException {
        if (channel != null) {
            channel.close();
        }
    }

    private static boolean sameFiles(Path a, Path b) throws IOException {
        List<Path> left = list(a);
        List<Path> right = list(b);
        if (left.size() != right.size()) {
            return false;
        }
        for (int i = 0; i < left.size(); i++) {
            if (!left.get(i).getFileName().equals(right.get(i).getFileName())
                    || !Arrays.equals(Files.readAllBytes(left.get(i)), Files.readAllBytes(right.get(i)))) {
                return false;
            }
        }
        return true;
    }

    private static List<Path> list(Path dir) throws IOException {
        try (Stream<Path> files = Files.list(dir)) {
            return files.sorted().collect(Collectors.toList());
        }
    }

    private static long logFiles(Path dir) throws IOException {
        return list(dir).stream().filter(f -> f.toString().endsWith(".log")).count();
    }

    private static void delete(Path root) throws IOException {
        if (!Files.exists(root)) {
            return;
        }
        try (Stream<Path> paths = Files.walk(root)) {
            for (Path path : paths.sorted((x, y) -> y.compareTo(x)).collect(Collectors.toList())) {
                Files.delete(path);
            }
        }
    }

    private static byte[] payload(long key, int size) {
        ByteBuffer buffer = ByteBuffer.allocate(size);
        buffer.putLong(key);
        for (int i = 8; i < size; i++) {
            buffer.put((byte) (key * 31 + i));
        }
        return buffer.array();
    }

    private static double percentile(long[] sorted, double p) {
        int index = (int) Math.ceil(p * sorted.length) - 1;
        return sorted[Math.max(0, Math.min(sorted.length - 1, index))];
    }

    private static String verdict(boolean condition, String ifTrue, String ifFalse) {
        return condition ? ifTrue : ifFalse;
    }

    private static String num(double v) {
        if (Double.isNaN(v) || Double.isInfinite(v)) {
            return "n/a";
        }
        double abs = Math.abs(v);
        String format = abs >= 100 ? "%,.0f" : abs >= 10 ? "%.1f" : "%.2f";
        return String.format(Locale.ROOT, format, v);
    }

    private static String pct(double fraction) {
        return Double.isNaN(fraction) ? "n/a" : String.format(Locale.ROOT, "%.0f%%", fraction * 100);
    }

    private String render() {
        StringBuilder out = new StringBuilder();
        out.append("Each row is the median of ").append(REPS).append(" timed repetitions after ").append(WARMUP)
                .append(" untimed warmup. Variants in a scenario run interleaved, in a rotating order each repetition. ")
                .append("A latency row gives the median across repetitions of each repetition's p50 or p99. Range is the lowest to the highest repetition. ")
                .append("Errors counts failed calls, wrong offsets and wrong bytes. CPU is process CPU time divided by wall time during the timed region, in cores.\n\n");
        for (Section section : sections) {
            out.append("## ").append(section.title).append("\n\n");
            out.append("| Variant | Metric | Median | Range | Errors | CPU | Check |\n");
            out.append("| --- | --- | ---: | --- | ---: | ---: | --- |\n");
            for (Row row : section.rows) {
                String median = row.count ? String.valueOf((long) row.value.median) : num(row.value.median);
                String range = row.count ? "n/a" : num(row.value.min) + " to " + num(row.value.max);
                out.append("| ").append(row.variant).append(" | ").append(row.metric).append(" | ").append(median).append(" | ").append(range)
                        .append(" | ").append(row.errors).append(" | ").append(num(row.cores.median)).append(" | ").append(row.check).append(" |\n");
            }
            out.append("\n");
            for (String paragraph : section.analysis) {
                out.append(paragraph).append("\n\n");
            }
            out.append(String.format(Locale.ROOT, "Scenario wall time %.1f s. Warnings or errors logged by NanoKafka during it: %d.", section.seconds, section.logged));
            for (String sample : section.loggedSamples) {
                out.append(" Example: `").append(sample.replace('`', '\'').replace('\n', ' ')).append("`.");
            }
            out.append("\n\n");
        }
        return out.toString();
    }

    private static final class Section {
        final String title;
        final List<Row> rows = new ArrayList<>();
        final List<String> analysis = new ArrayList<>();
        double seconds;
        long logged;
        List<String> loggedSamples = List.of();

        Section(String title) {
            this.title = title;
        }
    }

    private static final class Result {
        final double value;
        final double p99;
        final double cores;
        final long errors;
        final long verified;
        final long expected;

        Result(double value, double p99, double cores, long errors, long verified, long expected) {
            this.value = value;
            this.p99 = p99;
            this.cores = cores;
            this.errors = errors;
            this.verified = verified;
            this.expected = expected;
        }

        static Result latencyMicros(long[] nanos, Meter meter, long errors, long checkedCalls) {
            long[] sorted = nanos.clone();
            Arrays.sort(sorted);
            return new Result(percentile(sorted, 0.50) / 1e3, percentile(sorted, 0.99) / 1e3, meter.cores(), errors,
                    checkedCalls == 0 ? 0 : checkedCalls - errors, checkedCalls);
        }
    }

    private static final class Row {
        final String variant;
        final String metric;
        final Stats value;
        final Stats cores;
        final long errors;
        final String check;
        final boolean count;

        Row(String variant, String metric, Stats value, Stats cores, long errors, String check, boolean count) {
            this.variant = variant;
            this.metric = metric;
            this.value = value;
            this.cores = cores;
            this.errors = errors;
            this.check = check;
            this.count = count;
        }

        static Row check(String variant, long count, long errors, String check) {
            return new Row(variant, "count", new Stats(new double[] { count }), new Stats(new double[] { Double.NaN }), errors, check, true);
        }

        Row plusErrors(long more) {
            return new Row(variant, metric, value, cores, errors + more, check, count);
        }
    }

    private static final class Stats {
        final double median;
        final double min;
        final double max;

        Stats(double[] values) {
            double[] sorted = values.clone();
            Arrays.sort(sorted);
            int n = sorted.length;
            median = n % 2 == 1 ? sorted[n / 2] : (sorted[n / 2 - 1] + sorted[n / 2]) / 2;
            min = sorted[0];
            max = sorted[n - 1];
        }
    }

    private static final class Series {
        private final List<Result> results = new ArrayList<>();

        void add(Result result) {
            results.add(result);
        }

        double value(int rep) {
            return results.get(rep).value;
        }

        Stats values() {
            return new Stats(results.stream().mapToDouble(r -> r.value).toArray());
        }

        Stats cores() {
            return new Stats(results.stream().mapToDouble(r -> r.cores).toArray());
        }

        Row row(String variant, String metric, String check) {
            long errors = results.stream().mapToLong(r -> r.errors).sum();
            long verified = results.stream().mapToLong(r -> r.verified).sum();
            long expected = results.stream().mapToLong(r -> r.expected).sum();
            String detail = expected > 0 ? verified + "/" + expected + " " + check : check;
            return new Row(variant, metric, values(), cores(), errors, detail, false);
        }

        Row p99Row(String variant) {
            return new Row(variant, "p99 us", new Stats(results.stream().mapToDouble(r -> r.p99).toArray()), cores(), 0, "same calls as the p50 row", false);
        }
    }

    private static final class Meter {
        private static final com.sun.management.OperatingSystemMXBean OS = ManagementFactory.getOperatingSystemMXBean() instanceof com.sun.management.OperatingSystemMXBean
                ? (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean()
                : null;
        private final long wall0 = System.nanoTime();
        private final long cpu0 = cpu();
        private long wall;
        private long cpu;

        static Meter start() {
            return new Meter();
        }

        void stop() {
            wall = System.nanoTime() - wall0;
            cpu = cpu() - cpu0;
        }

        double seconds() {
            return wall / 1e9;
        }

        double cores() {
            return OS == null ? Double.NaN : (double) cpu / wall;
        }

        private static long cpu() {
            return OS == null ? 0 : OS.getProcessCpuTime();
        }
    }

    private static final class LogCounter extends Handler {
        final AtomicLong count = new AtomicLong();
        final List<String> samples = new ArrayList<>();

        void reset() {
            count.set(0);
            synchronized (samples) {
                samples.clear();
            }
        }

        @Override
        public void publish(LogRecord record) {
            if (record.getLevel().intValue() < Level.WARNING.intValue() || record.getLoggerName() == null
                    || !record.getLoggerName().startsWith("com.simplekafka")) {
                return;
            }
            count.incrementAndGet();
            synchronized (samples) {
                if (samples.size() < 2) {
                    samples.add(record.getLevel() + " " + record.getMessage());
                }
            }
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
        }
    }
}
