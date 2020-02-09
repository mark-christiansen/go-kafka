package avro

import (
    "encoding/binary"
    "github.com/linkedin/goavro/v2"
    "github.com/mark-christiansen/go-kafka/confluent"
    "github.com/mark-christiansen/go-kafka/kafka"
    "github.com/pkg/errors"
    "time"
)

type MessageConsumer struct {
    consumer *confluent.MessageConsumer
    schemaRegistryClient *kafka.SchemaRegistryClient
}

func NewMessageConsumer(consumer *confluent.MessageConsumer, schemaRegistryUrl, caCertFilepath string) (*MessageConsumer, error){

    schemaRegistryClient, err := kafka.NewSchemaRegistryClient(schemaRegistryUrl, caCertFilepath)
    if err != nil {
        return nil, errors.Wrap(err, "Error creating Avro message consumer")
    }
    return &MessageConsumer {
        consumer: consumer,
        schemaRegistryClient: &schemaRegistryClient,
    }, nil
}

func (c *MessageConsumer) Subscribe(topic string) {
    consumer := *c.consumer
    consumer.Subscribe(topic)
}

func (c *MessageConsumer) Read(timeout time.Duration) (kafka.Message, error) {

    msg, err := (*c.consumer).Read(timeout)
    if err != nil {
        return nil, errors.Wrap(err, "Error reading message from consumer")
    }

    client := *c.schemaRegistryClient

    keySchemaId, keyBytes, err := getSchemaIdAndMessage(msg.Key())
    _, keyCodec, err := client.GetSchema(keySchemaId)
    if err != nil {
        return nil, errors.Wrap(err, "Error getting schema for key from schema registry")
    }

    valueSchemaId, valueBytes, err := getSchemaIdAndMessage(msg.Value())
    _, valueCodec, err := client.GetSchema(valueSchemaId)
    if err != nil {
        return nil, errors.Wrap(err, "Error getting schema for value from schema registry")
    }

    avroMsg := NewMessage(keyBytes, valueBytes, msg.Timestamp(), msg.Headers(), keySchemaId, keyCodec, valueSchemaId, valueCodec)
    return avroMsg, nil
}

func (c *MessageConsumer) Close() {
    (*c.consumer).Close()
}

func (c *MessageConsumer)  IsOpen() bool {
    return (*c.consumer).IsOpen()
}

func (c *MessageConsumer) Commit() error {
    return (*c.consumer).Commit()
}

func (c *MessageConsumer) AutoCommit() bool {
    return (*c.consumer).AutoCommit()
}

func getSchemaIdAndMessage(bytes []byte) (schemaId int, message []byte, err error){

    if len(bytes) == 0 {
        return -1, nil, errors.New("Avro message is empty, cannot get schema ID")
    }

    if len(bytes) < 6 {
        return -1, nil, errors.New("Avro message is malformed, no magic byte or schema ID")
    }

    if bytes[0] != 0x0 {
        return -1, nil, errors.New("Avro message is malformed, first byte is not the expected magic byte")
    }

    schemaId = int(binary.BigEndian.Uint32(bytes[1:5]))
    message = bytes[5:]
    return schemaId, message, nil
}

func deserialize(codec *goavro.Codec, bytes []byte) (interface{}, []byte, error) {
    return codec.NativeFromBinary(bytes)
}
