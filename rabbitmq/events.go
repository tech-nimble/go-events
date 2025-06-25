package rabbitmq

import (
	"context"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/tech-nimble/go-events"
)

const defaultPublishTimeout = 1 * time.Second

type ExchangeOptions struct {
	Name       string
	Type       string
	Durable    bool
	Internal   bool
	AutoDelete bool
	NoWait     bool
	Args       map[string]interface{}
}

type Exchange struct {
	client  *RabbitMQ
	Name    string
	Options ExchangeOptions
}

func (e *Exchange) Publish(message events.Publishing) error {
	body, err := message.GetBody()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultPublishTimeout)
	defer cancel()

	return e.client.channel.PublishWithContext(
		ctx,
		e.Name,
		message.GetRoutingKey(),
		false,
		false,
		amqp.Publishing{
			Headers:      message.GetHeaders(),
			Body:         body,
			DeliveryMode: uint8(message.GetDeliveryMode()),
		},
	)
}

type Events struct {
	client    *RabbitMQ
	exchanges []*Exchange
}

func Init(client *RabbitMQ) (*Events, error) {
	e := &Events{
		client: client,
	}

	return e, nil
}

func (e *Events) AddExchange(
	name, exchangeType string,
	durable, autoDelete, internal, noWait bool,
	args map[string]interface{},
) error {
	if !e.client.GetStatus() {
		return ErrConnectionClose
	}

	ch := e.client.GetChannel()

	if ch == nil {
		return ErrConnectionClose
	}

	exch := &Exchange{
		client: e.client,
		Name:   name,
		Options: ExchangeOptions{
			Name:       name,
			Type:       exchangeType,
			Durable:    durable,
			Internal:   internal,
			AutoDelete: autoDelete,
			NoWait:     noWait,
			Args:       args,
		},
	}

	err := ch.ExchangeDeclare(
		exch.Name,
		exch.Options.Type,
		exch.Options.Durable,
		exch.Options.AutoDelete,
		exch.Options.Internal,
		exch.Options.NoWait,
		exch.Options.Args,
	)
	if err != nil {
		return err
	}

	e.exchanges = append(e.exchanges, exch)

	return nil
}

func (e *Events) GetExchange(name string) *Exchange {
	for _, exchange := range e.exchanges {
		if exchange.Name == name {
			return exchange
		}
	}

	return nil
}
