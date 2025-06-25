package rabbitmq

import amqp "github.com/rabbitmq/amqp091-go"

type QueueOptions struct {
	// Name queue name
	Name string

	// DeleteWhenUnused sets the auto-delete flag. It's recommended to have this
	// set to false so that amqp-rpc can reconnect and use the same queue while
	// keeping any messages in the queue.
	DeleteWhenUnused bool

	// Durable sets the durable flag. It's recommended to have this set to false
	// and instead use ha-mode for queues and messages.
	Durable bool

	// Exclusive sets the exclusive flag when declaring queues. This flag has
	// no effect on Clients reply-to queues which are never exclusive so it
	// can support reconnects properly.
	Exclusive bool

	// When noWait is true, the queue will assume to be declared on the server.  A
	// channel exception will arrive if the conditions are met for existing queues
	// or attempting to modify an existing queue from a different connection.
	NoWait bool

	// Args sets the arguments table used.
	Args amqp.Table
}

type BindOptions struct {
	// Key ключ для связывания routing key
	Key string

	// Exchange name
	Exchange    string
	NoWait      bool
	BindHeaders amqp.Table
}

// declareQueueAndBind will declare a queue, an exchange and the queue to the exchange.
func declareQueueAndBind(ch *amqp.Channel, queueOptions *QueueOptions, bindOptions []*BindOptions) error {
	if ch == nil {
		return errChannelNotCreated
	}

	queue, err := ch.QueueDeclare(
		queueOptions.Name,
		queueOptions.Durable,
		queueOptions.DeleteWhenUnused,
		queueOptions.Exclusive,
		queueOptions.NoWait, // no-wait.
		queueOptions.Args,
	)
	if err != nil {
		return err
	}

	if len(bindOptions) == 0 {
		return nil
	}

	for _, bindOption := range bindOptions {
		if err = ch.QueueBind(
			queue.Name,
			bindOption.Key,
			bindOption.Exchange,
			bindOption.NoWait,
			bindOption.BindHeaders,
		); err != nil {
			return err
		}
	}

	return nil
}
