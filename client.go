package events

import amqp "github.com/rabbitmq/amqp091-go"

const (
	Transient DeliveryMode = 1 + iota
	Persistent
)

type DeliveryMode uint8

type Publisher interface {
	Publish(publishing Publishing) error
}

type Publishing interface {
	GetBody() ([]byte, error)
	GetHeaders() amqp.Table
	SetHeaders(map[string]interface{})
	GetDeliveryMode() DeliveryMode
	GetRoutingKey() string
}
