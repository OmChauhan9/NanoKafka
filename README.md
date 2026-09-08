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

## How to Run (Coming Soon)
*Instructions will be added as the core broker logic is implemented.*
