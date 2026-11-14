# NanoKafka benchmarks

`scripts/bench.sh` measures the broker's storage, request, and replication paths and writes a Markdown report to `bench/results/<UTC date>-<short git sha>.md`. Every number in a report comes from one run of that script on the machine named in its header.

## Run the benchmarks

You need a JDK 11 or newer, Maven, and git. From the repository root, run:

```sh
scripts/bench.sh
```

The script builds the main and test classes, runs `com.simplekafka.bench.Bench` from a fresh temp directory, prints the results tables, and deletes the temp directory. A run took under three minutes on the machine in the first report. If `JAVA_HOME` is set, the script uses that JDK. Otherwise it uses `java` from `PATH`.

Commit your changes before you run it. The report records the git SHA and whether the working tree was dirty, and a number from a dirty tree describes code that no commit holds.

The harness lives in `src/test/java/com/simplekafka/bench/Bench.java`. Surefire runs only `*Test` classes, so `mvn verify` compiles the harness but never runs it.

## What each scenario measures

Every scenario runs one untimed warmup repetition and then 5 timed repetitions. Within a scenario, the variants run interleaved, and the order rotates each repetition, so drift on the machine favors no variant.

- **a. Partition append throughput.** One thread calls `Partition.append` 200 times on a fresh partition with 100-byte messages, then with 1 KB messages. Every offset is then read back and compared byte for byte. The same repetitions also time `FileChannel.force(true)` on its own from 1, 4, and 16 threads, plus a log force followed by an index force, which is the I/O an append does with no Partition code around it.
- **b. Partition single-message read latency.** The scenario reads one 1 KB message at a random offset in the first, middle, and last segment of partitions with 2, 11, and 20 segments, 2,000 reads per repetition. A 20-segment partition holds about 20 MB. Each read is checked against the bytes written at that offset. The 2-segment partition is built with `Partition.append`. The larger ones are written straight to disk in the same format, and the report shows that the direct writer's files are byte-identical to the files `append` writes. A last row times opening, reading, and closing one segment file with no Partition code.
- **c. End-to-end latency.** A 2-broker `TestCluster` (embedded ZooKeeper and real brokers on free localhost ports) serves a topic at replication factor 1 and one at replication factor 2. The scenario times `SimpleKafkaClient.send` with 100-byte messages, `SimpleKafkaClient.fetch` of one message by offset, and a metadata request that does no disk work. An in-process `Partition.append` runs in the same repetitions as a reference. Afterward, the scenario fetches every acked offset from the leader. For replication factor 2, it also fetches every acked offset straight from the follower.
- **d. Concurrent produce throughput.** 1, 4, and 16 client threads send 100-byte messages to a 1-partition topic and a 4-partition topic on the same cluster. Thread i sends to partition i mod P with its own client. Afterward, the scenario checks that every acked (partition, offset) pair is unique and fetches back the bytes sent. A force pair measurement runs in the same repetitions to give each variant its disk ceiling.

## Read a report

The header records the machine: OS and version, CPU model, logical core count, RAM, JDK vendor and version, the git SHA, whether the tree was dirty, the UTC date, the data directory's filesystem, the loopback port limits, and the load average when the script started. It records no hostname, username, or home path.

Each scenario has one table. Each column holds the following:

- **Median** is the median of the 5 timed repetitions. For a latency row, it is the median of each repetition's p50 or p99.
- **Range** is the lowest and the highest repetition. Treat a difference smaller than the range as no measurable difference.
- **Errors** counts failed calls, wrong offsets, and wrong bytes. Any value above 0 means the scenario did not do the work it timed, and the script exits with status 1.
- **CPU** is process CPU time divided by wall time during the timed region, in cores. A value far below 1 on a single-threaded row means the thread spends most of its time waiting, not computing.
- **Check** says how the scenario proved the work happened, with the number of verified items.

Under each table, a paragraph names the limiter and does the arithmetic behind it. For example, scenario a compares the force pair's time with an append's time from the same repetition. A sentence marked inconclusive names what the run could not establish. Each section ends with the scenario's wall time and the number of warnings or errors NanoKafka logged during it.

## Check a limiter with a profiler

The report infers limiters from timings measured side by side. To see where the time goes, profile a run that you do not report, because a profiler slows the work it watches. JFR comes with the JDK. Add `-XX:StartFlightRecording=filename=bench.jfr,settings=profile` to the `java` command in `scripts/bench.sh`. Then read the samples of threads blocked in native code:

```sh
jfr print --events jdk.NativeMethodSample --stack-depth 8 bench.jfr
```

For the first recorded result, a separate profiled program that ran 1,600 appends on the same machine put 297 of the 321 native samples under `Partition.append` inside `FileDispatcherImpl.force0`. The log force took 178 and the index force 119. The rest were 14 samples in `RandomAccessFile.open0` and 10 in `FileDispatcherImpl.write0`.
