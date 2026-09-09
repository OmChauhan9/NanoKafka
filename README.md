# Build Your Own Kafka

A simplified, educational implementation of a Kafka-like distributed streaming platform built from scratch in Java.

## Project Goal
The goal of this project is to understand the core architecture of Apache Kafka by building a simplified version. It helps in mastering distributed systems concepts such as:
- Distributed Append-Only Logs
- Pub-Sub Messaging
- Partitioning and Replication
- Wire Protocols and Network Communication

## Day 1 Progress
- **Project Setup**: Initialized Maven project (`pom.xml`) with ZooKeeper dependency.
- **Broker Representation**: Implemented `BrokerInfo.java` to track active brokers in the cluster.
- **Wire Protocol**: Designed the custom byte-level communication protocol (`Protocol.java`) to handle how clients and brokers talk to each other over the network (Produce, Fetch, Metadata, Create Topic).

## Day 2 Progress
- **Cluster Coordination**: Implemented `ZookeeperClient.java` to manage cluster metadata, handle ephemeral broker registration, and react to cluster events using Watchers.
- **Storage Engine**: Implemented `Partition.java` to handle the append-only log mechanism. Includes segment rollover, index-based O(1) message lookups, and fast binary search for offset resolution.

## Day 3 Progress
- **Network & Routing**: Implemented `SimpleKafkaBroker.java` with a non-blocking network server to accept client connections and route bytes to handlers.
- **Controller & Election**: Built logic for Controller election using ZooKeeper. The Controller handles new topic creation and partition rebalancing when a broker crashes.
- **Data Flow & Replication**: Implemented request handlers for Produce, Fetch, and Metadata. Built active replication, where the Leader actively pushes copies of data to Follower brokers.

## How to Run (Coming Soon)
*Instructions will be added as the core broker logic is implemented.*
