package kafka

import "time"

type Message interface {
    Key() []byte
    Value() []byte
    Timestamp() time.Time
    Headers() []Header
}

type DefaultMessage struct {
    value          []byte
    key            []byte
    timestamp      time.Time
    headers        []Header
}

type Header struct {
    Key   string // KafkaHeader name (utf-8 string)
    Value []byte // KafkaHeader value (nil, empty, or binary)
}

func NewMessage(key []byte, value []byte, timestamp time.Time) *DefaultMessage {
    return &DefaultMessage{key: key, value: value, timestamp: timestamp, headers: make([]Header, 0)}
}

func NewHeader(key string, value []byte) Header {
    return Header{Key: key, Value: value}
}

func (m *DefaultMessage) Key() []byte {
    return m.key
}

func (m *DefaultMessage) Value() []byte {
    return m.value
}

func (m *DefaultMessage) Timestamp() time.Time {
    return m.timestamp
}

func (m *DefaultMessage) Headers() []Header {
    return m.headers
}

func (m *DefaultMessage) AddHeader(key string, value []byte) {
    header := NewHeader(key, value)
    m.headers = append(m.headers, header)
}