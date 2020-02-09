package kafka

import (
    "time"
)

type MessageConsumer interface {
    Subscribe(topic string)
    Read(timeout time.Duration) (Message, error)
    Close()
    IsOpen() bool
    Commit() error
    AutoCommit() bool
}
