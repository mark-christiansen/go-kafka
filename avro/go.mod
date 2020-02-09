module github.com/mark-christiansen/go-kafka/avro

require (
    github.com/linkedin/goavro/v2 v2.9.7
    github.com/mark-christiansen/go-kafka/confluent v0.0.1
    github.com/mark-christiansen/go-kafka/kafka v0.0.1
    github.com/pkg/errors v0.9.1
)

replace github.com/mark-christiansen/go-kafka/confluent => ../confluent

replace github.com/mark-christiansen/go-kafka/kafka => ../kafka

go 1.13
