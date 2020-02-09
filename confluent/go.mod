module github.com/mark-christiansen/go-kafka/confluent

require (
	github.com/confluentinc/confluent-kafka-go v1.3.0
	github.com/mark-christiansen/go-kafka/kafka v0.0.1
	github.com/pkg/errors v0.9.1
)

replace github.com/mark-christiansen/go-kafka/kafka => ../kafka

go 1.13
