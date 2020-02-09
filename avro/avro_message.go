package avro

import (
    "github.com/linkedin/goavro/v2"
    "github.com/mark-christiansen/go-kafka/kafka"
    "time"
)

type Message struct {
    value          []byte
    key            []byte
    timestamp      time.Time
    headers        []kafka.Header
    keySchemaId    int
    keyCodec       *goavro.Codec
    valueSchemaId  int
    valueCodec     *goavro.Codec
}

func NewMessage(key []byte, value []byte, timestamp time.Time, headers []kafka.Header, keySchemaId int, keyCodec *goavro.Codec, valueSchemaId int, valueCodec *goavro.Codec) *Message {
    return &Message{key: key, value: value, timestamp: timestamp, headers: headers, keySchemaId: keySchemaId, keyCodec: keyCodec, valueSchemaId: valueSchemaId, valueCodec: valueCodec}
}

func (m *Message) Key() []byte {
    return m.key
}

func (m *Message) Value() []byte {
    return m.value
}

func (m *Message) Timestamp() time.Time {
    return m.timestamp
}

func (m *Message) Headers() []kafka.Header {
    return m.headers
}