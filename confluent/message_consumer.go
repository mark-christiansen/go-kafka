/*
Confluent implementation of Kafka client
 */
package confluent

import (
    "fmt"
    "time"

    "github.com/confluentinc/confluent-kafka-go/kafka"
    opKafka "github.com/mark-christiansen/go-kafka/kafka"
)

// A Confluent Kafka message consumer
type MessageConsumer struct {
    consumer *kafka.Consumer // the underlying Confluent Kafka message consumer
    open bool
    autoCommit bool
    schemaRegistryClient *opKafka.SchemaRegistryClient
}

// Create new Confluent Kafka message consumer
func NewMessageConsumer(groupId, clientId, autoOffsetReset string, autoCommit bool) *MessageConsumer {
    c := setupConsumer(groupId, clientId, autoOffsetReset, autoCommit)
    return &MessageConsumer{consumer: c, open: true, autoCommit: autoCommit}
}

// Subscribe the consumer to the specified topic name
func (c *MessageConsumer) Subscribe(topic string) {
    err := c.consumer.SubscribeTopics([]string{topic, "^aRegex.*[Tt]opic"}, nil)
    if err != nil {
        fmt.Printf("Error subscribing to topic: %#v\n", err)
    }
}

// Read a message from Kafka until first message received or times out
func (c *MessageConsumer) Read(timeout time.Duration) (opKafka.Message, error) {
    if m, err := c.consumer.ReadMessage(timeout); err == nil {
        msg := convert(m)
        return msg, err
    } else {
        return nil, err
    }
}

// Close the message consumer
func (c *MessageConsumer) Close() {
    c.consumer.Close()
    c.open = false
}

func (c *MessageConsumer) IsOpen() bool {
    return c.open
}

func (c *MessageConsumer) Commit() error {
    partitions, err := c.consumer.Commit()
    if err == nil {
        for _, partition := range partitions {
            fmt.Printf( "Committed Topic: %#v, Partition: %#v, Offset:  %#v\n", partition.Topic, partition.Partition, partition.Offset)
        }
    }
    return err
}

func (c *MessageConsumer) AutoCommit() bool {
    return c.autoCommit
}

func setupConsumer(groupId, clientId, autoOffsetReset string, autoCommit bool) *kafka.Consumer {
    consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
        "bootstrap.servers": "localhost:9092,localhost:9093,localhost:9094,localhost:9095,localhost:9096",
        "group.id":          groupId,
        "auto.offset.reset": autoOffsetReset,
        "client.id": clientId,
        "enable.auto.commit": autoCommit,
        // frequency in milliseconds that the consumer offsets are auto-committed to Kafka
        //"auto.commit.interval.ms": 5000,
        // expected time between heartbeats to the consumer coordinator
        // heartbeats are used to ensure that the consumer's session stays active and to facilitate rebalancing when
        // new consumers join or leave the group.
        //"heartbeat.interval.ms": 3000,
        // maximum amount of data per-partition the server will return
        //"max.partition.fetch.bytes": 1048576,
        // timeout used to detect consumer failures
        // If no heartbeats are received by the broker before the expiration of this session timeout, then the broker
        // will remove this consumer from the group and initiate a rebalance.
        //"session.timeout.ms": 10000,
        // maximum delay between invocations of poll() when using consumer group management, if poll() is not called
        // before expiration of this timeout, then the consumer is considered failed and the group will rebalance
        //"max.poll.interval.ms": 300000,
        // maximum number of records returned in a single call to poll()
        //"max.poll.records": 500,
        // minimum amount of data the server should return for a fetch request, if insufficient data is available the
        // request will wait for that much data to accumulate before answering the request
        //"fetch.min.bytes": 1,
        // maximum amount of data the server should return for a fetch request
        //"fetch.max.bytes": 52428800,
        // maximum amount of time the server will block before answering the fetch request if there isn't sufficient
        // data to immediately satisfy the requirement given by fetch.min.bytes
        //"fetch.max.wait.ms": 500,
        // maximum amount of time the client will wait for the response of a request, if not received before the timeout
        // elapses the client will resend the request if necessary or fail the request if retries are exhausted
        //"request.timeout.ms": 305000,
    })
    if err != nil {
        panic(err)
    }
    return consumer
}

func convert(m *kafka.Message) opKafka.Message {
    message := opKafka.NewMessage(m.Key, m.Value, m.Timestamp)
    for _, h := range m.Headers {
        message.AddHeader(h.Key, h.Value)
    }
    return message
}