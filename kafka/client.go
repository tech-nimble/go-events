package kafka

import (
	"crypto/md5"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/tech-nimble/go-events"
)

type Kafka struct {
	brokers  string
	producer *kafka.Producer
	consumer *kafka.Consumer
}

func NewKafka(brokers string) *Kafka {
	return &Kafka{
		brokers: brokers,
	}
}

func (k *Kafka) GetProducer() (*kafka.Producer, error) {
	if k.producer == nil {
		err := k.RunProducer()
		if err != nil {
			return nil, err
		}
	}

	return k.producer, nil
}

func (k *Kafka) RunProducer() error {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": k.brokers})

	if err != nil {
		k.producer = nil

		return err
	}

	k.producer = producer

	return nil
}

// TODO: Добавить консьюмера
func (k *Kafka) RunConsumer() {}

func (k *Kafka) Close() (err error) {
	if k.producer != nil {
		k.producer.Close()
	}

	if k.consumer != nil {
		err = k.consumer.Close()
	}

	return
}

func getHeaders(headers amqp.Table) []kafka.Header {
	var h []kafka.Header

	for k, v := range headers {
		var b []byte
		switch vt := v.(type) {
		case string:
			b = []byte(vt)
		case []byte:
			b = vt
		}
		h = append(h, kafka.Header{
			Key:   k,
			Value: b,
		})
	}

	return h
}

func getMessageKey(message events.Publishing) []byte {
	b, _ := message.GetBody()

	return []byte(fmt.Sprintf("[%s] %s-%d", message.GetRoutingKey(), md5.New().Sum(b), time.Now().Unix()))
}
