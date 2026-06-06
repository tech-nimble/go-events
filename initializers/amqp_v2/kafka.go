// SPDX-FileCopyrightText: 2025 Nimble Tech
// SPDX-License-Identifier: MIT

package amqp_v2

import (
	"github.com/gobuffalo/envy"
	"github.com/tech-nimble/go-events/kafka"
)

const (
	KafkaBrokersEnv = "KAFKA_BROKERS"
)

func InitializeKafka() *kafka.Kafka {
	return kafka.NewKafka(envy.Get(KafkaBrokersEnv, ""))
}

func InitializeKafkaPublisher(client *kafka.Kafka) (*kafka.Producer, error) {
	err := client.RunProducer()
	if err != nil {
		return nil, err
	}

	return kafka.NewProducer(client), nil
}
