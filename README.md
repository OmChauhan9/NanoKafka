# Build Your Own Kafka

[![CI](https://github.com/OmChauhan9/NanoKafka/actions/workflows/ci.yml/badge.svg)](https://github.com/OmChauhan9/NanoKafka/actions/workflows/ci.yml)

A simplified, educational implementation of a Kafka-like distributed streaming platform, written in Java, following the Build Your Own Kafka tutorial.

The code is adapted from the [Build Your Own Kafka](https://github.com/buildthingsuseful/build-your-own-kafka) tutorial by Raghavendra Nandagopal, used under the MIT License. See [LICENSE](LICENSE).

This project is a way to study how Kafka works by running and changing a small version of it. It covers these ideas:

- Distributed append-only logs
- Pub-sub messaging
- Partitioning and replication
- Wire protocols and network communication

## Run a two-broker cluster

You need JDK 11 or later, Maven 3.9 or later, and bash. Older Maven 3 releases are untested. The first build downloads dependencies, so it needs network access. Ports 2181, 9092, and 9093 must be free. Run each command from the repository root.

1. Build the jar and run the tests.

	```sh
	mvn package
	```

	The build is done when it prints `BUILD SUCCESS`, after 30 to 90 seconds. The tests print many `SEVERE` lines and stack traces on purpose, because they stop brokers and ZooKeeper sessions. A first build also prints a few harmless `maven-shade-plugin` warnings about overlapping resources, and more if you build again without `mvn clean`.

	If tests fail with `Can't assign requested address`, the machine ran out of ephemeral ports. Wait about 30 seconds and run `mvn package` again. Do not run the build and `scripts/bench.sh` at the same time.

2. In a new terminal, start ZooKeeper on port 2181. The script reuses the test classpath, which holds the two libraries that ZooKeeper's server needs.

	```sh
	scripts/zookeeper.sh
	```

	ZooKeeper is ready when it prints a line containing `binding to port 0.0.0.0/0.0.0.0:2181`. It keeps its data in `${TMPDIR:-/tmp}/nanokafka-zookeeper`. The first run of the script also downloads one Maven plugin without printing anything. To use another port or data directory, run `scripts/zookeeper.sh <port> <data directory>`, and pass the same port as the fourth argument to each broker. The reset command below removes only the default data directory.

3. Start broker 1 in a new terminal, after ZooKeeper is ready. A broker started without ZooKeeper retries forever and fills its terminal with connection errors.

	```sh
	java -jar target/build-your-own-kafka-1.0-SNAPSHOT.jar 1 localhost 9092
	```

	The broker is ready when it prints `SimpleKafka broker started. Press Ctrl+C to stop.` It writes its data to `data/1` under the current directory. An optional fourth argument sets the ZooKeeper port, which defaults to 2181.

4. Start broker 2 in another new terminal, and wait for the same ready line.

	```sh
	java -jar target/build-your-own-kafka-1.0-SNAPSHOT.jar 2 localhost 9093
	```

5. In a fourth terminal, send 10 messages to the topic `demo`. The producer creates the topic with 3 partitions if it does not exist. It picks a random partition for each message and sends one message per second.

	```sh
	java -cp target/build-your-own-kafka-1.0-SNAPSHOT.jar com.simplekafka.client.SimpleKafkaProducer localhost 9092 demo
	```

	The producer prints `Sent message to offset: <n>` once for each message. Each partition counts its own offsets, so the same offset appears more than once. After the 10th message it prints `Producer finished sending messages` and exits. If you run it again without the reset below, the topic keeps the earlier messages, so the consumer shows more.

6. In the same terminal, read partition 0 of `demo`. Press Enter to stop the consumer.

	```sh
	java -cp target/build-your-own-kafka-1.0-SNAPSHOT.jar com.simplekafka.client.SimpleKafkaConsumer localhost 9092 demo 0
	```

	The consumer prints `Received message at offset <n>: Message ...` for each message in partition 0. That is roughly a third of the 10 messages. Run the command again with `1` and then `2` in place of `0` to read the rest.

To stop, press Ctrl+C in each terminal. To start over from an empty cluster, also delete the broker and ZooKeeper data.

```sh
rm -rf data "${TMPDIR:-/tmp}/nanokafka-zookeeper"
```

## What works

Each item names the test that proves it.

- `Partition` stores each partition as an append-only log, split into segments that roll over at 1 MB. (`PartitionTest`)
- On restart, a partition drops a record that a crash cut short. It rebuilds the active segment's index, and the index of any older segment whose index file is missing. (`PartitionTest`)
- An append fails, and leaves no record, when its index file cannot be opened for writing. A message the leader rejects is never sent to followers. (`PartitionTest`, `RejectedAppendTest`)
- Each segment has a dense index, so finding a message's byte position is one seek. A binary search over segment base offsets picks the segment. On an Apple M3 Pro, a single-message read takes about 23 µs at any position in partitions of 2 to 20 segments, with the files in the page cache. Larger partitions and cold reads are untested. (`scripts/bench.sh`)
- `Protocol` defines the binary messages. Every request and response starts with a 4-byte length, so messages larger than one network read work. A frame holds at most 16 MiB. The broker rejects a message too large to reach every follower, and keeps every fetch response within one frame. (`WireFramingTest`, `FetchFrameLimitTest`)
- The broker serves each connection on its own thread with blocking sockets. (`ConnectionHandlingTest`)
- The controller handles topic creation. It replies once every live broker has loaded the new topic, so a produce to any partition right after succeeds. A broker that has not loaded it within 5 s does not hold up the reply. On one controller, concurrent creates of one name produce exactly one topic. Names are limited to ASCII letters, digits, '.', '_' and '-'. Names that differ from an existing topic only in case are rejected. (`CreateTopicTest`, `CreateTopicReadyTest`, `TopicNameTest`)
- The controller is the broker that creates ZooKeeper's ephemeral `/controller` node. When a dead session still holds that node, a new election runs once the session ends. A restarted broker registers again once its old session ends. (`ControllerRestartTest`, `StaleBrokerSessionTest`, `StaleControllerSessionTest`)
- When a leader stops, the controller makes a live replica of the partition its leader and tells every broker. A partition with no live replica has no leader until one returns. After a crash, the move waits for the dead broker's 30 s ZooKeeper session to expire. (`LeaderFailoverTest`, `ControllerRestartTest`)
- The leader appends each message, queues a copy for each live follower, and replies without waiting for the copies. (`ReplicationTest` covers the copies. That the reply does not wait comes from the code, not a test.)
- `SimpleKafkaClient` sends each request to the partition leader and caches metadata. When it cannot connect to the leader, it reloads metadata from its bootstrap broker and retries once. A broker answers a fetch only for a partition it leads, and the client also reloads metadata and retries once when a fetch fails, so a reader that still names a former leader reads from the new one. One client can be shared across threads. (`LeaderFailoverTest`, `ClientMetadataRefreshTest`)
- `SimpleKafkaProducer` creates its topic if needed and spreads its messages across partitions, picking each partition at random. (`ProducerTest` covers the spread. The random choice comes from the code.)
- `SimpleKafkaConsumer` fetches continuously on a background thread, tracks its offset, and supports `seek`. It keeps running through a broker restart. (`ConsumerTest`)

## Known limits

- Failover can lose messages. The leader replies before followers have a copy, and the controller does not check that the new leader has every message. Messages acknowledged before a failure can disappear, and their offsets can be used again.
- When a broker's own ZooKeeper session expires, the broker neither registers again nor restores its watches. The cluster treats it as dead while it keeps serving. A controller that stalls past its session timeout can keep acting as controller beside a new one.
- A follower appends each copy at its own next offset. A follower that is down when a message arrives never receives it, so every later message lands at a different offset than on the leader. Under concurrent producers, a follower can also store messages in a different order.
- A broker that misses the controller's notification about a new topic, or a new leader, catches up only when it restarts or the topic is reassigned again.
- Consumer offsets live in memory only. A restarted consumer starts from the offset you give it.
- Every append flushes the log and the index to disk. That caps one partition near 190 messages per second on an Apple M3 Pro.
- The client opens a new TCP connection for every request, and a produce with replication factor N opens N connections. With macOS defaults of 16,384 ephemeral ports and a 30 s TIME_WAIT, that allows about 546 new connections per second for the whole host. That figure is computed, not measured.
- The client reads metadata only from its bootstrap broker. If that broker is down, the client cannot refresh metadata.
- Client reads and calls between brokers have no timeout. A broker that hangs while it keeps running blocks whoever waits on it. That can be a client, or a broker that forwards a produce or a topic create.
- If an index write fails partway, after the index file opened, the log record is removed but a stale index entry can remain. Later reads in that segment can then return the wrong record until a restart rebuilds the index.
- A broker that joins while a topic is being created can hold only part of that topic. It catches up when a later notice for the topic, such as a leader change, reaches it.
- On restart, an older segment with an index that exists but is wrong is trusted as it is.
- Logs are never deleted. There is no retention by time or size.

## Future work

None of these is built yet. Each one would remove limits from the list above, and each names the test or benchmark that would prove it.

### Replication that survives a leader crash

Followers would fetch from the leader at the leader's offsets, instead of the leader pushing copies. The leader would track which followers are caught up, called the in-sync replicas, and a high watermark, which is the last offset that every in-sync replica has stored. A producer could ask for `acks=all`, which would make the leader reply only after every in-sync replica has the message. Consumers would read only up to the high watermark. The controller would pick a new leader only from the in-sync replicas, and a leader epoch would tell a follower which of its records to discard after a failover.

This would remove three limits: lost messages on failover for messages sent with `acks=all`, reused offsets, and followers that store messages at different offsets. A crash test would kill the leader again and again under concurrent producers. It would then check that no acknowledged message is lost or duplicated, and that every replica ends up byte-identical. `scripts/bench.sh` would report the latency cost of `acks=all` over `acks=1`.

### Group commit for disk flushes

Today every append flushes the log and the index on its own. With group commit, appends that arrive close together would share one flush, and each producer would still get its reply only after its message is on disk. The producer could also send several messages in one request.

This would target the limit of about 190 messages per second per partition. `scripts/bench.sh` would measure the change before and after on the same machine. A durability test would stop the broker in the middle of a batch and check that every acknowledged message is still there after a restart.

### Consumer groups with saved offsets

Several consumers would share the partitions of a topic. Each consumer would commit its offset, so a restarted consumer would continue where it stopped. When a consumer joins or leaves, the partitions would be assigned again.

This would remove the limit that consumer offsets live in memory only. Tests would add, stop, and restart consumers, then check that every message is delivered, and that a message is delivered twice only in documented cases.

## Benchmarks

`scripts/bench.sh` builds the code, runs four scenarios, and writes a report to `bench/results/`. Each report records the machine, the JDK, and the git commit. [bench/README.md](bench/README.md) explains each scenario.

The committed report, [2026-11-14-86f678e](bench/results/2026-11-14-86f678e.md), comes from an Apple M3 Pro with Corretto 17. Each number is the median of 5 timed repetitions within one run, and every repetition had 0 errors. That report was recorded at `86f678e`, before the review panel's fixes. A second report, [2026-11-15-57e13cf](bench/results/2026-11-15-57e13cf.md), was recorded on the same machine after the round 1 fixes, and every row in the table below stayed within or near its committed range.

| Measurement | Result | Limited by |
| --- | --- | --- |
| Append, one thread, 1 KB messages | 184 messages/s | Two disk flushes per message |
| Read one message | 23 µs at p50 | Opening the index and log files |
| Produce through two brokers, replication factor 1 | 6.9 ms at p50 | The leader's append |
| Produce through two brokers, replication factor 2 | 10.4 ms at p50 | Two appends on one disk, inferred from the arithmetic, not profiled |
| Fetch one message | 117 µs at p50 | The network round trip |
| Concurrent produce, 16 threads on 4 partitions | 240 messages/s | The disk's flush rate |

## Layout

The Java sources are under `src/main/java/com/simplekafka/`.

- `broker/Partition.java` stores one partition as segments of `.log` and `.index` files.
- `broker/Protocol.java` defines the binary messages and their framing.
- `broker/SimpleKafkaBroker.java` serves requests, runs the controller, and copies messages to followers.
- `broker/ZookeeperClient.java` wraps the ZooKeeper calls that the broker makes.
- `broker/BrokerInfo.java` holds one broker's id, host, and port.
- `client/SimpleKafkaClient.java` sends requests to the right leader and caches metadata.
- `client/SimpleKafkaProducer.java` sends strings to random partitions, and `client/SimpleKafkaConsumer.java` polls one partition. Both have a command-line entry point.
- `src/test/java/com/simplekafka/TestCluster.java` runs ZooKeeper and real brokers inside one JVM for the tests.
- `scripts/zookeeper.sh` starts ZooKeeper for the run guide, and `scripts/bench.sh` runs the benchmarks.
- `bench/` holds the benchmark guide and the recorded reports.
