package avro

import (
	"bytes"
	"fmt"
	"github.com/linkedin/goavro/v2"
	"github.com/mark-christiansen/go-kafka/confluent"
	"github.com/mark-christiansen/go-kafka/kafka"
	"github.com/pkg/errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

const (
	KeySchemaSuffix = "-key"
	ValueSchemaSuffix = "-value"
	SchemaPackage = "objectpartners.com/machrist/schema/"
)

type MessageProducer struct {
	producer *confluent.MessageProducer
	schemaRegistryClient *kafka.SchemaRegistryClient
}

func NewMessageProducer(producer *confluent.MessageProducer, schemaRegistryUrl, caCertFilepath string) (*MessageProducer, error){

	schemaRegistryClient, err := kafka.NewSchemaRegistryClient(schemaRegistryUrl, caCertFilepath)
	if err != nil {
		return nil, errors.Wrap(err, "Error creating Avro message consumer")
	}
	return &MessageProducer {
		producer: producer,
		schemaRegistryClient: &schemaRegistryClient,
	}, nil
}

func (p *MessageProducer) Send(topic string, key []byte, value []byte, flush bool) error {

	client := *p.schemaRegistryClient
	keyBuf := bytes.NewBuffer(make([]byte, 0, 1024))
	valueBuf := bytes.NewBuffer(make([]byte, 0, 1024))

	err := getAvroMessageBytes(topic + KeySchemaSuffix, client, key, keyBuf)
	if err != nil {
		return err
	}
	keyBytes := keyBuf.Bytes()
	keyBuf.Reset()

	err = getAvroMessageBytes(topic + ValueSchemaSuffix, client, value, valueBuf)
	if err != nil {
		return err
	}
	valueBytes := valueBuf.Bytes()
	valueBuf.Reset()

	return p.producer.Send(topic, keyBytes, valueBytes, flush)
}

func (p *MessageProducer) Flush() {
	p.producer.Flush()
}

func (p *MessageProducer) Close() {
	p.producer.Close()
}

func (p *MessageProducer) IsOpen() bool {
	return p.producer.IsOpen()
}

func getAvroMessageBytes(subject string, client kafka.SchemaRegistryClient, messageBytes []byte, buf *bytes.Buffer) (error) {

	schemaId, err := client.GetSchemaId(subject)
	if err != nil {

		// if the schema is not found in the schema registry, register the local version of the schema in this application
		if strings.HasSuffix(err.Error(), "Subject not found.") {

			schemaLocalFilepath, err := filepath.Abs(os.Getenv("GOPATH")  + "/src/" + SchemaPackage + subject + ".avsc")
			if err != nil {
				return errors.Wrap(err, fmt.Sprintf("Could not find local schema file at %s", schemaLocalFilepath))
			}

			schema, err := ioutil.ReadFile(schemaLocalFilepath)
			if err != nil {
				return errors.Wrap(err, fmt.Sprintf("Could not find schema for subject %s in schema registry or locally", subject))
			}

			schemaId, err = client.RegisterSchema(subject, string(schema))
			if err != nil {
				return errors.Wrap(err, fmt.Sprintf("Could not register schema for subject %s in schema registry", subject))
			}
		}
	}

	err = addSchemaId(messageBytes, schemaId, buf)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("Error adding schema ID %d to message for schema subject %s", schemaId, subject))
	}
	return nil
}

func addSchemaId(messageBytes []byte, schemaId int, buf *bytes.Buffer) (err error){

	if len(messageBytes) == 0 {
		return errors.New("Avro message is empty, cannot append schema ID")
	}

	// write Avro magic byte to byte buffer
	buf.WriteByte(0x0)
	// write schema ID to byte buffer as big endian int
	i := uint32(schemaId)
	buf.WriteByte(byte(i >> 24))
	buf.WriteByte(byte(i >> 16))
	buf.WriteByte(byte(i >> 8))
	buf.WriteByte(byte(i))

	buf.Write(messageBytes)
	return nil
}

func serialize(codec *goavro.Codec, native interface{}) ([]byte, error) {
	return codec.BinaryFromNative(nil, native)
}