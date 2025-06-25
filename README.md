# Библиотека для работы с протоколом amqp

## Подписка на события

Для подписки на события используется Consumer

```go
package main

import (
	"github.com/tech-nimble/go-events"
)

func main() {
	consumer := rabbitmq.NewConsumer("amqp://guest:guest@rabbitmq:5672/")

	consumer.
		WithConsumerOptions(&rabbitmq.ConsumerOptions{
			Consumer:         "cosumer-name",
			QoSPrefetchCount: 5,
		}).
		WithQueueOptions(&rabbitmq.QueueOptions{
			Name:    "cosumer-queue-name",
			Durable: true,
		}).
		WithBindOptions(&rabbitmq.BindOptions{
			Key:      "routing-key",
			Exchange: "exchange-name",
		})

	consumer.ListenAndServe()
    
	// Логика приложения
	
	consumer.Close()
}
```


## TODO
- Сделать graceful shutdown
