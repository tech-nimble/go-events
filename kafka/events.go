package kafka

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/tech-nimble/go-events"
)

type Producer struct {
	client *Kafka
}

func NewProducer(client *Kafka) *Producer {
	return &Producer{client: client}
}

func (e *Producer) Publish(message events.Publishing) error {
	body, err := message.GetBody()
	if err != nil {
		return err
	}

	producer, err := e.client.GetProducer()
	if err != nil {
		return err
	}

	topic := message.GetRoutingKey()

	return producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value:   body,
		Key:     getMessageKey(message),
		Headers: getHeaders(message.GetHeaders()),
	}, nil)
}
