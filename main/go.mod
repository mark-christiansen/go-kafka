module github.com/mark-christiansen/go-kafka/main

require (
    github.com/brianvoe/gofakeit v3.18.0+incompatible
    github.com/mark-christiansen/go-kafka/avro v0.0.1
    github.com/mark-christiansen/go-kafka/confluent v0.0.1
    github.com/mark-christiansen/go-kafka/kafka v0.0.1
    github.com/mark-christiansen/go-kafka/schema v0.0.1
)

replace github.com/mark-christiansen/go-kafka/avro => ../avro

replace github.com/mark-christiansen/go-kafka/confluent => ../confluent

replace github.com/mark-christiansen/go-kafka/kafka => ../kafka

replace github.com/mark-christiansen/go-kafka/schema => ../schema

go 1.13
