package kafka

import (
    "crypto/tls"
    "crypto/x509"
    "fmt"
    "github.com/landoop/schema-registry"
    "github.com/linkedin/goavro/v2"
    "github.com/pkg/errors"
    "io/ioutil"
    "net/http"
    "sync"
)

type SchemaRegistryClient interface {
    GetSchema(schemaId int) (schema string, codec *goavro.Codec, err error)
    GetSchemaId(subject string) (schemaId int, err error)
    RegisterSchema(subject string, schema string) (int, error)
}

type DefaultSchemaRegistryClient struct {
    client *schemaregistry.Client
    subjectCache map[string]int
    schemaCache map[int]string
    codecCache map[int]*goavro.Codec
    mutex sync.Mutex
}

func NewSchemaRegistryClient(url string, caCertFilepath string) (SchemaRegistryClient, error) {

    var client *schemaregistry.Client
    var err error

    if caCertFilepath != "" {

        caCert, err := ioutil.ReadFile(caCertFilepath)
        if err != nil {
            return nil, err
        }

        caCertPool := x509.NewCertPool()
        caCertPool.AppendCertsFromPEM(caCert)

        config := &tls.Config{
            RootCAs: caCertPool,
        }
        transport := &http.Transport {
            TLSClientConfig: config,
        }
        httpsClient := &http.Client{
            Transport: transport,
        }

        client, err = schemaregistry.NewClient(url, schemaregistry.UsingClient(httpsClient))
        if err != nil {
            return nil, errors.Wrap(err, fmt.Sprintf("Error connecting to schema registry using url %s and CA cert %s", url, caCertFilepath))
        }
    } else {
        client, err = schemaregistry.NewClient(url)
        if err != nil {
            return nil, errors.Wrap(err, fmt.Sprintf("Error connecting to schema registry using url %s", url))
        }
    }

    return &DefaultSchemaRegistryClient {
        client:      client,
        subjectCache: map[string]int{},
        schemaCache: map[int]string{},
        codecCache:  map[int]*goavro.Codec{},
        mutex:       sync.Mutex{},
    }, nil
}

func (c *DefaultSchemaRegistryClient) GetSchema(schemaId int) (schema string, codec *goavro.Codec, err error) {

    c.mutex.Lock()
    defer c.mutex.Unlock()

    schema, found := c.schemaCache[schemaId]
    if !found {
        schema, err = c.client.GetSchemaByID(schemaId)
        if err != nil {
            return "", nil, errors.Wrap(err, fmt.Sprintf("Error getting schema (id=%d) from schema registry", schemaId))
        }
        c.schemaCache[schemaId] = schema
    }

    codec, found = c.codecCache[schemaId]
    if !found {
        codec, err = goavro.NewCodec(schema)
        if err != nil {
            return "", nil, errors.Wrap(err, fmt.Sprintf("Error creating codec for schema (id=%d)", schemaId))
        }
        c.codecCache[schemaId] = codec
    }

    return schema, codec, nil
}

func (c *DefaultSchemaRegistryClient) GetSchemaId(subject string) (schemaId int, err error) {

    schemaId, found := c.subjectCache[subject]
    if !found {

        schema, err := c.client.GetLatestSchema(subject)
        if err != nil {
            return -1, err
        }
        c.subjectCache[subject] = schema.ID
        return schema.ID, nil
    }

    return schemaId, nil
}

func (c *DefaultSchemaRegistryClient) RegisterSchema(subject string, schema string) (int, error) {
    schemaId, err := c.client.RegisterNewSchema(subject, schema)
    if err != nil {
        return -1, err
    }
    return schemaId, nil
}