Introduction
============

This is an event delivery implementation in Go using native Kafka constructs
for supporting the desired system's requirements.

- **Durability:** Received events through HTTP are stored inside a Kafka topic
  and each destination handler only commits offsets after the
  successful processing of an event. after a restart/crash,
  consumers shall resume from the last commited offset. New consumer groups
  will reprocess the full event log.

- **At-least-once delivery:** Kafka provides at-least-once guarantees
  as long as you you make sure that you don't commit offsets for
  unprocessed events.

- **Retry backoff and limit:** Since we care about ordering guarantees,
  there is no simple way to implement a retry mechanism using kafka's
  native constructs such as additional topics. This implementation uses an in-memory retry
  mechanism during event polling which has the following side effects:

  1. Retry counters will be reset upon rebalancing or process restart,
     so the implemented system offers at-least n-retry guarantees.
  2. Delays or failures with a single event delivery, affect the delivery
     of all other events behind the problematic event in the same partition.

- **Maintaining order:** Kafka can guarantee the order of events in the
  same partition for a given consumer group. We simply have to make
  sure that events with the same UserID will be sent to the same partition,
  which will happen if we use the UserID as the message's key and keep
  the default partitioner which calculates destination partitions based
  on key-hashing.

- **Delivery isolation:** Using a separate consumer group per destination
  provides us with both the required parallelism and delivery isolation
  between destinations, since each consumer group corresponds to a
  separate process which can consume events independently from other
  consumer groups.

# Compromises

1. You cannot use more consumers than you have partitions available to
   read from, choose wisely!

2. Even with several partitions, you cannot achieve the performance
   levels obtained by per-key parallel processing which unfortunatelly
   is not natively supported by kafka.

3. A single slow or failing message will block all messages behind the
   problematic message, ie. the entire partition. The process may recover,
   but the latency of all the messages behind the problematic one will be
   negatively impacted severely.

Running E2E tests
=================

1. Run Kafka locally at `localhost:9092`
2. Port `8080` should be available for the web server to successfully bind, also make sure that there is no local firewall setup blocking access
3. Run `go test -v ./e2e`

Further Work
============

The system's requirements would be better served if we implemented a Go version of [Confluent's Parallel Consumer](https://www.confluent.io/blog/introducing-confluent-parallel-message-processing-client/) which is written in [Java](https://github.com/confluentinc/parallel-consumer) and implements a client side queueing system on top of Apache Kafka consumer. The following features are especially attractive:

1. We can have more parallelism than the topic's partition configuration supports
2. We can use message level acknowledgment instead of partition level offsets, i.e. achieve per-key concurrent processing

The task of porting this library in Go is of course out of scope of the current assignment, given its complexity.

Another option worth investigating is using __Apache Pulsar__ instead of Apache Kafka as the streaming platform. In contrast to Kafka, Pulsar supports natively all the following features:

1. `Key_Shared` subscription mode
2. Advanced message retention policies, e.g. delete messages acknowledged by consumers
3. Parallelism not limited to the number of partitions
3. Retry & dead-letter topics
