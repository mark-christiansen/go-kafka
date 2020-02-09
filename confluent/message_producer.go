/*
Confluent implementation of Kafka client
*/
package confluent

import (
    "fmt"
    "github.com/confluentinc/confluent-kafka-go/kafka"
    "github.com/pkg/errors"
)

const flushTimeout = -1

// A Confluent Kafka message producer
type MessageProducer struct {
    producer *kafka.Producer // the underlying Confluent Kafka message producer
    open bool
}

// Create new Confluent Kafka message producer
func NewProducer() *MessageProducer {
    p := setupProducer()
    return &MessageProducer{producer: p, open: true}
}

// Send a message with a key to the topic specified
func (p *MessageProducer) Send(topic string, key []byte, message []byte, flush bool) error {

    err := p.producer.Produce(&kafka.Message{
        TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
        Key:            key,
        Value:          message,
    }, nil)

    if err != nil {
        return errors.Wrapf(err, "Error sending message to topic %s", topic)
    }

    if flush {
        p.producer.Flush(flushTimeout)
    }
    return nil
}

// Flush the producer message buffer
func (p *MessageProducer) Flush() {
    p.producer.Flush(flushTimeout)
}

// Close the message producer
func (p *MessageProducer) Close() {
    p.producer.Close()
    p.open = false
}

func (p *MessageProducer) IsOpen() bool {
    return p.open
}

func setupProducer() *kafka.Producer {

    producer, err := kafka.NewProducer(&kafka.ConfigMap {
        "bootstrap.servers": "localhost:9092,localhost:9093,localhost:9094,localhost:9095,localhost:9096",
        "message.max.bytes": 1024 * 1024 * 10,
        //"go.batch.producer": true,
        // number of acks the producer requires the leader to have received before considering a request complete
        "acks": "all",
        // default batch size in bytes
        //"batch.size": 16384,
        // if not set may change the ordering if batch #1 fails and batch #2 succeeds
        "max.in.flight.requests.per.connection": 1,
        //"retries": 10,
        // max size of request in bytes, will limit the number of record batches the producer will send in a single request
        //"max.request.size": 1048576,
        // maximum amount of time the client will wait for the response of a request
        "request.timeout.ms": 30000,
        // compression type for all data generated by the producer
        "compression.type": "lz4",
        "default.topic.config":  kafka.ConfigMap{
            "acks": "all",
            "request.timeout.ms": 5000,
        },
    })
    if err != nil {
        panic(err)
    }

    // Delivery report handler for produced messages
    go func() {
        for e := range producer.Events() {
            switch ev := e.(type) {
            case *kafka.Message:
                if ev.TopicPartition.Error != nil {
                    fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
                }
            }
        }
    }()

    return producer
}