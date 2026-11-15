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

You need JDK 11 or later and Maven 3.6 or later. Run each command from the repository root, and use a new terminal for each long-running process.

1. Build the jar and run the tests.

	```sh
	mvn package
	```

2. Start ZooKeeper on port 2181. The script reuses the test classpath, which holds the two libraries that ZooKeeper's server needs.

	```sh
	scripts/zookeeper.sh
	```

3. Start two brokers. Each broker writes its data to `data/<broker id>` under the current directory.

	```sh
	java -jar target/build-your-own-kafka-1.0-SNAPSHOT.jar 1 localhost 9092
	java -jar target/build-your-own-kafka-1.0-SNAPSHOT.jar 2 localhost 9093
	```

4. Send 10 messages to the topic `demo`. The producer creates the topic with 3 partitions if it does not exist, and sends one message per second.

	```sh
	java -cp target/build-your-own-kafka-1.0-SNAPSHOT.jar com.simplekafka.client.SimpleKafkaProducer localhost 9092 demo
	```

	The producer prints `Sent message to offset: <n>` once for each message.

5. Read partition 0 of `demo`. Press Enter to stop the consumer.

	```sh
	java -cp target/build-your-own-kafka-1.0-SNAPSHOT.jar com.simplekafka.client.SimpleKafkaConsumer localhost 9092 demo 0
	```

	The consumer prints `Received message at offset <n>: Message ...` for each message in the partition.

## What works

Each item names the test that proves it.

- `Partition` stores each partition as an append-only log, split into segments that roll over at 1 MB. (`PartitionTest`)
- On restart, a partition drops a record that a crash cut short, and it rebuilds an index that is short or missing. (`PartitionTest`)
- Each segment has a dense index, so finding a message's byte position is one seek. A binary search over segment base offsets picks the segment. A single-message read takes about 23 µs on an Apple M3 Pro, whatever the message's position. (`scripts/bench.sh`)
- `Protocol` defines the binary messages. Every request and response starts with a 4-byte length, so messages larger than one network read work. (`WireFramingTest`)
- The broker serves each connection on its own thread with blocking sockets. (`ConnectionHandlingTest`)
- The controller handles topic creation. Two concurrent creates of one name produce exactly one topic, and names that are not safe as paths are rejected. (`CreateTopicTest`, `TopicNameTest`)
- ZooKeeper elects one controller. A restarted broker registers again once its old session ends. (`ControllerRestartTest`, `StaleBrokerSessionTest`, `StaleControllerSessionTest`)
- When a broker dies, the controller moves its partitions to a live broker. (`LeaderFailoverTest`, `ControllerRestartTest`)
- The leader appends each message, replies to the producer, then sends a copy to each follower in the background. (`ReplicationTest`)
- `SimpleKafkaClient` sends each request to the partition leader. It caches metadata and reads it again when the leader stops answering. (`LeaderFailoverTest`)
- `SimpleKafkaProducer` creates its topic if needed and picks a random partition for each message. (`ProducerTest`)
- `SimpleKafkaConsumer` fetches continuously on a background daemon thread, tracks its offset, and supports `seek`. It keeps running through a broker restart. (`ConsumerTest`)

## Known limits

- Failover can lose messages. The leader replies before followers have a copy, and the controller does not check that the new leader has every message. Messages acknowledged before a failure can disappear, and their offsets can be used again.
- Only the controller learns about a new leader. Other brokers keep reporting the old leader.
- When a broker's own ZooKeeper session expires, the broker does not restore its watches. A controller that stalls past its session timeout can keep acting as controller beside a new one.
- Under concurrent producers, a follower can store a partition's messages in a different order from its leader, because a follower appends at its own next offset.
- Consumer offsets live in memory only. A restarted consumer starts from the offset you give it.
- Every append flushes the log and the index to disk. That caps one partition near 190 messages per second on an Apple M3 Pro.
- The client opens a new TCP connection for every request. On macOS defaults, that caps sustained load near 546 requests per second.
- Client reads have no timeout. A broker that hangs while it keeps running blocks the client that is waiting on it.
- On restart, an older segment with an index that exists but is wrong is trusted as it is.

## Benchmarks

`scripts/bench.sh` builds the code, runs four scenarios, and writes a report to `bench/results/`. Each report records the machine, the JDK, and the git commit. [bench/README.md](bench/README.md) explains each scenario.

The committed report, [2026-11-14-86f678e](bench/results/2026-11-14-86f678e.md), comes from an Apple M3 Pro with Corretto 17. Each number is the median of 5 runs, and every run had 0 errors.

| Measurement | Result | Limited by |
| --- | --- | --- |
| Append, one thread, 1 KB messages | 184 messages/s | Two disk flushes per message |
| Read one message | 23 µs at p50 | Opening the index and log files |
| Produce through two brokers, replication factor 1 | 6.9 ms at p50 | The leader's append |
| Produce through two brokers, replication factor 2 | 10.4 ms at p50 | Two appends on one disk |
| Fetch one message | 117 µs at p50 | The network round trip |
| Concurrent produce, 16 threads on 4 partitions | 240 messages/s | The disk's flush rate |

## Layout

- `broker/Partition.java` stores one partition as segments of `.log` and `.index` files.
- `broker/Protocol.java` defines the binary messages and their framing.
- `broker/SimpleKafkaBroker.java` serves requests, runs the controller, and copies messages to followers.
- `broker/ZookeeperClient.java` wraps the ZooKeeper calls that the broker makes.
- `broker/BrokerInfo.java` holds one broker's id, host, and port.
- `client/SimpleKafkaClient.java` sends requests to the right leader and caches metadata.
- `client/SimpleKafkaProducer.java` sends strings to random partitions, and `client/SimpleKafkaConsumer.java` polls one partition. Both have a command-line entry point.
- `src/test/java/com/simplekafka/TestCluster.java` runs ZooKeeper and real brokers inside one JVM for the tests.
