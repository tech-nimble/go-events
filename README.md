# go-events

[![CI](https://github.com/tech-nimble/go-events/actions/workflows/ci.yml/badge.svg)](https://github.com/tech-nimble/go-events/actions/workflows/ci.yml)

AMQP/Kafka event toolkit for Go services: a domain event model, an outbox-style
event bus, a RabbitMQ consumer/publisher, an RPC client/server and a Kafka
producer.

## Install

```sh
go get github.com/tech-nimble/go-events
```

## Subscribe to events

```go
consumer := rabbitmq.NewConsumer("amqp://guest:guest@rabbitmq:5672/")

consumer.
	WithConsumerOptions(&rabbitmq.ConsumerOptions{
		Consumer:         "consumer-name",
		QoSPrefetchCount: 5,
	}).
	WithQueueOptions(&rabbitmq.QueueOptions{
		Name:    "consumer-queue-name",
		Durable: true,
	}).
	WithBindOptions(&rabbitmq.BindOptions{
		Key:      "routing-key",
		Exchange: "exchange-name",
	})

_ = consumer.AddHandler("order.created", func(ctx context.Context, msg amqp.Delivery) {
	// handle message
})

if err := consumer.ListenAndServe(); err != nil {
	log.Fatal().Err(err).Msg("consumer failed")
}
defer consumer.Close()
```

## Routing

Messages are matched to handlers by a `Router`. `HeaderRouter` (default) routes
by the `type` header; `RouterKeyRouter` routes by the AMQP routing key. Both
support strict, regexp or custom matchers.

## Packages

| Package | Purpose |
| --- | --- |
| (root) | `Event` model, `EventBus`, `Router`, DB outbox repository. |
| `rabbitmq` | Connection, consumer, publisher and RPC client/server. |
| `kafka` | Kafka producer. |
| `initializers/amqp_v2` | Env-based bootstrap helpers for RabbitMQ, Kafka and the event bus. |

## License

[MIT](LICENSE) © Nimble Tech
