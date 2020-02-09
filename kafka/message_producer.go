package kafka

type MessageProducer interface {
    Send(topic string, key []byte, message []byte, flush bool) error
    Flush()
    Close()
    IsOpen() bool
}