// SPDX-FileCopyrightText: 2025 Nimble Tech
// SPDX-License-Identifier: MIT

package amqp_v2

import (
	"github.com/gobuffalo/envy"
	"github.com/tech-nimble/go-events/rabbitmq"
)

const (
	AMQPUrlEnv          = "AMQP_URL"
	AMQPExchangeNameEnv = "AMQP_EXCHANGE_NAME"
	AMQPExchangeTypeEnv = "AMQP_EXCHANGE_TYPE"
)

func InitializeRabbitMQ() (*rabbitmq.RabbitMQ, error) {
	client := rabbitmq.NewRabbitMQ(
		&rabbitmq.Options{
			URL:             envy.Get(AMQPUrlEnv, ""),
			ConnectAttempts: 5,
		})

	if err := client.Init(); err != nil {
		return nil, err
	}

	return client, nil
}

func InitializeRabbitMQPublisher(client *rabbitmq.RabbitMQ) (*rabbitmq.Exchange, error) {
	ev, err := rabbitmq.Init(client)
	if err != nil {
		return nil, err
	}

	exchangeName := envy.Get(AMQPExchangeNameEnv, "default")
	exchangeType := envy.Get(AMQPExchangeTypeEnv, "topic")

	err = ev.AddExchange(
		exchangeName,
		exchangeType,
		false,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		return nil, err
	}

	return ev.GetExchange(exchangeName), nil
}
