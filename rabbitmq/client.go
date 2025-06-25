package rabbitmq

import (
	"errors"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog/log"
)

var ErrConnectionClose = errors.New("Соединение с RabbitMQ закрыто ")

type RabbitMQ struct {
	name            string
	connection      *amqp.Connection
	channel         *amqp.Channel
	options         *Options
	connectionError chan *amqp.Error
	notifyClose     chan bool
	notifyReConsume chan bool
	isClosed        bool
}

type Options struct {
	Url             string
	ConnectAttempts int
}

func NewRabbitMQ(options *Options) *RabbitMQ {
	return &RabbitMQ{
		options:         options,
		notifyReConsume: make(chan bool, 1),
		notifyClose:     make(chan bool, 1),
	}
}

func (r *RabbitMQ) Init() error {
	if r.isConnectionActive() {
		return nil
	}

	var err error

	if err = r.connect(); err == nil {
		go r.watchReconnect()
	}

	return err
}

func (r *RabbitMQ) isConnectionActive() bool {
	return r.connection != nil && r.channel != nil
}

func (r *RabbitMQ) GetStatus() bool {
	return !r.isClosed
}

func (r *RabbitMQ) GetName() string {
	return r.name
}

func (r *RabbitMQ) Close() {
	r.close()

	r.notifyClose <- true
	defer close(r.notifyClose)
	defer close(r.notifyReConsume)
}

func (r *RabbitMQ) close() {
	if r.connection != nil {
		r.connection.Close()
	}

	if r.channel != nil {
		r.channel.Close()
	}
}

func (r *RabbitMQ) GetConnection() *amqp.Connection {
	if r.isClosed {
		return nil
	}

	return r.connection
}

func (r *RabbitMQ) GetChannel() *amqp.Channel {
	if r.isClosed {
		return nil
	}

	return r.channel
}

func (r *RabbitMQ) connect() error {
	var err error

	r.close()

	if r.connection, err = amqp.Dial(r.options.Url); err != nil {
		return err
	}

	if r.channel, err = r.connection.Channel(); err != nil {
		return err
	}

	r.notifyClose = make(chan bool, 1)

	return nil
}

func (r *RabbitMQ) watchReconnect() {
	r.connectionError = make(chan *amqp.Error)
	r.channel.NotifyClose(r.connectionError)

rcn:
	for {
		select {
		case connErr := <-r.connectionError:
			if err := r.tryReconnect(r.options.ConnectAttempts); err != nil {
				log.Fatal().
					Err(connErr).
					Int("attempt", r.options.ConnectAttempts).
					Msg("reconnect failed")
				r.Close()
				break rcn
			}
		case <-r.notifyClose:
			break rcn
		}
	}
}

func (r *RabbitMQ) tryReconnect(attempts int) error {
	r.isClosed = true

	for i := 0; i < attempts; i++ {
		if err := r.connect(); err == nil {
			r.connectionError = make(chan *amqp.Error)
			r.channel.NotifyClose(r.connectionError)

			r.isClosed = false
			r.notifyReConsume <- true

			return nil
		}

		time.Sleep(time.Duration(attempts) * time.Second)
	}

	return errors.New("cannot reconnect to rabbitmq")
}
