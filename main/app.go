package main

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/brianvoe/gofakeit"
	"github.com/mark-christiansen/go-kafka/avro"
	"github.com/mark-christiansen/go-kafka/confluent"
	"github.com/mark-christiansen/go-kafka/kafka"
	"github.com/mark-christiansen/go-kafka/schema"
	"os"
	"strconv"
	"strings"
	"time"
)

const TopicName = "person"

type TestRunner struct {
	topicName string
	messages, producerBatchSize, consumerBatchSize int
	consumers []kafka.MessageConsumer
	consumerPauseTime time.Duration
	consumerGroupId string
	producers []kafka.MessageProducer
	producerPauseTime time.Duration
}

func main() {

	t := TestRunner{topicName: TopicName, messages: 5000000, producerBatchSize: 100, consumerBatchSize: 500,
		producers: make([]kafka.MessageProducer, 5), producerPauseTime: 100,
		consumers: make([]kafka.MessageConsumer, 20), consumerPauseTime: 0, consumerGroupId: "person-consumer-golang"}

	for i := 0; i < len(t.producers); i++ {

		kafkaProducer := confluent.NewProducer()
		avroProducer, err := avro.NewMessageProducer(kafkaProducer, "http://localhost:8081", "")
		if err != nil {
			fmt.Printf("Error creating Avro producer: %#v\n", err)
		}

		t.producers[i] = avroProducer
		go t.ProducePeople(i, t.producers[i])
	}

	for i := 0; i < len(t.consumers); i++ {

		kafkaConsumer := confluent.NewMessageConsumer(t.consumerGroupId, fmt.Sprintf("consumer-%d", i), "earliest", true)
		avroConsumer, err := avro.NewMessageConsumer(kafkaConsumer, "http://localhost:8081", "")
		if err != nil {
			fmt.Printf("Error creating Avro consumer: %#v\n", err)
		}

		t.consumers[i] = avroConsumer
		t.consumers[i].Subscribe(t.topicName)
		go t.ConsumePeople(i, t.consumers[i])
	}

	t.ConsumeCommands()
}

func (t *TestRunner) ConsumeCommands() {

	for true {
		reader := bufio.NewReader(os.Stdin)
		fmt.Print("Enter command (exit, stop, start): ")
		command, _ := reader.ReadString('\n')
		command = strings.TrimSuffix(command, "\n")
		fmt.Println(command)

		if command == "exit" {
			for _, consumer := range t.consumers {
				consumer.Close()
			}
			for _, producer := range t.producers {
				producer.Close()
			}
			break
		} else if command == "stop" || command == "start" {

			reader := bufio.NewReader(os.Stdin)
			fmt.Print("Enter entity (consumer, producer): ")
			entity, _ := reader.ReadString('\n')
			entity = strings.TrimSuffix(entity, "\n")
			fmt.Println(entity)

			fmt.Printf("Enter entity ID: ")
			entityIdStr, _ := reader.ReadString('\n')
			entityIdStr = strings.TrimSuffix(entityIdStr, "\n")
			fmt.Println(entityIdStr)
			entityId, _ := strconv.Atoi(entityIdStr)

			switch entity {
			case "consumer":
				if command == "stop" {
					t.consumers[entityId].Close()
				} else {
					t.consumers[entityId] = confluent.NewMessageConsumer(t.consumerGroupId, fmt.Sprintf("consumer-%d", entityId), "earliest", true)
					t.consumers[entityId].Subscribe(t.topicName)
					go t.ConsumePeople(entityId, t.consumers[entityId])
				}
			case "producer":
				if command == "stop" {
					t.producers[entityId].Close()
				} else {
					t.producers[entityId] = confluent.NewProducer()
					go t.ProducePeople(entityId, t.producers[entityId])
				}
			}
		}
	}
}

func (t *TestRunner) ProducePeople(id int, producer kafka.MessageProducer) {

	defer producer.Close()

	batches := t.producerBatches()
	batchCount := 0
	peeps := make([]*schema.Person, t.producerBatchSize)
	for batchCount < batches && producer.IsOpen() {
		peeps := t.createPeople(peeps)
		t.sendPeople(producer, peeps)
		fmt.Printf("Producer/Batch #%d/%d sent %d messages\n", id, batchCount, t.producerBatchSize)
		time.Sleep(t.producerPauseTime * time.Millisecond)
		batchCount++
	}
	fmt.Printf("Producer #%d sent %d batches and is stopping\n", id, batches)
}

func (t *TestRunner) ConsumePeople(id int, consumer kafka.MessageConsumer) {

	defer consumer.Close()

	batches := 0
	for consumer.IsOpen() {

		received := t.receivePeople(consumer, t.consumerBatchSize)
		if received != nil {

			fmt.Printf("Consumer #%d received %d messages\n", id, len(received))
			//for i, p := range received {
			//	if p != nil {
			//		fmt.Printf("Consumer #%d: Person #%d [Name=%s, Address=%s, Age=%d]\n", id, i, p.Name, p.Address.String, p.Age.Int)
			//	}
			//}

			if !consumer.AutoCommit() {
				err := consumer.Commit()
				if err != nil {
					fmt.Printf("Consumer/Batch #%d/%d error on commit: %#v\n", id, batches, err)
				}
			}
			batches++
		}

		time.Sleep(t.consumerPauseTime* time.Millisecond)
	}
	fmt.Printf("Consumer #%d received %d batches and is stopping\n", id, batches)
}

func (t *TestRunner) producerBatches() int {
	return t.messages / (len(t.producers) * t.producerBatchSize)
}

func (t *TestRunner) receivePeople(consumer kafka.MessageConsumer, count int) []*schema.Person {

	//fmt.Println("Reading messages...")
	peeps := make([]*schema.Person, count)

	for i := 0; i < count; i++ {
		//fmt.Printf("Reading message #%d\n", i)
		msg, err := consumer.Read(-1)
		if err != nil {
			fmt.Printf("Error occurred reading message #%d from topic \"%s\": %v (%v)\n", i, t.topicName, err, msg)
		} else {

			keyReader := bytes.NewReader(msg.Key())
			_, err := schema.DeserializePersonKey(keyReader)

			valueReader := bytes.NewReader(msg.Value())
			p, err := schema.DeserializePerson(valueReader)

			if err != nil {
				fmt.Printf("Error deserializing person from message #%d from topic \"%s\": %v\n", i, t.topicName, err)
			}
			peeps[i] = p
		}
	}
	return peeps
}

func (t *TestRunner) createPeople(peeps []*schema.Person) []*schema.Person {

	for i := 0; i < len(peeps); i++ {

		p := schema.NewPerson()
		p.Name = gofakeit.Name()
		p.Address = schema.NewUnionNullString()
		address := gofakeit.Address()
		p.Address.String = fmt.Sprintf("%s %s, %s %s", address.Street, address.City, address.State, address.Zip)
		p.Address.UnionType = schema.UnionNullStringTypeEnumString
		p.Age = schema.NewUnionNullInt()
		p.Age.Int = int32(gofakeit.Number(10, 100))
		p.Age.UnionType = schema.UnionNullIntTypeEnumInt
		peeps[i] = p
	}
	return peeps
}

func (t *TestRunner) sendPeople(producer kafka.MessageProducer, people []*schema.Person) {

	//fmt.Println("Sending messages...")
	keyWriter := bytes.NewBuffer(make([]byte, 0, 1024))
	valueWriter := bytes.NewBuffer(make([]byte, 0, 1024))
	for i, p := range people {

		//fmt.Printf("Sending message #%d\n", i)

		// create and serialize person key to bytes
		key := schema.NewPersonKey()
		key.Name = p.Name
		err := key.Serialize(keyWriter)
		if err != nil {
			fmt.Printf("Error serializing person key for message #%d from topic \"%s\": %v\n", i, t.topicName, err)
		}

		// serialize person to bytes
		err = p.Serialize(valueWriter)
		if err != nil {
			fmt.Printf("Error serializing person for message #%d from topic \"%s\": %v\n", i, t.topicName, err)
		}

		producer.Send(t.topicName, keyWriter.Bytes(), valueWriter.Bytes(), false)

		keyWriter.Reset()
		valueWriter.Reset()
	}
	//fmt.Println("Flushing messages...")
	producer.Flush()
}