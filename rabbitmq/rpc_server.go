// SPDX-FileCopyrightText: 2025 Nimble Tech
// SPDX-License-Identifier: MIT

package rabbitmq

import (
	"context"
	"errors"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog/log"

	"github.com/tech-nimble/go-events"
)

const headerRouteKey = "type"

var ErrHandlerNameBusy = errors.New("handler with this name is already registered")

type Request struct {
	body    []byte
	headers map[string]any
}

func (r *Request) GetBody() ([]byte, error) {
	return r.body, nil
}

func (r *Request) GetHeaders() map[string]any {
	return r.headers
}

func (r *Request) SetHeaders(m map[string]any) {
	r.headers = m
}

// GetExchangeName returns an empty string: an incoming request is routed by
// header, not by exchange, so the value is not carried on the request.
func (r *Request) GetExchangeName() string {
	return ""
}

// GetCommandName returns an empty string: the command is resolved from the
// routing header rather than the request body.
func (r *Request) GetCommandName() string {
	return ""
}

type Handler struct {
	client *RabbitMQ
	name   string
	handle Handle
}

type Handle func(ctx context.Context, command events.Command) events.Response

type RPCServer struct {
	client         *RabbitMQ
	Handlers       []*Handler
	queueOptions   *QueueOptions
	workers        int
	reconnectDelay int
}

func NewRPCServer(client *RabbitMQ, queueName string, workers, reconnectDelay int) *RPCServer {
	server := &RPCServer{
		client: client,
		queueOptions: &QueueOptions{
			Name:    queueName,
			Durable: true,
		},
		workers:        workers,
		reconnectDelay: reconnectDelay,
	}

	return server
}

func (r *RPCServer) Init() error {
	if err := r.initQueue(); err != nil {
		return err
	}

	return nil
}

func (r *RPCServer) Run() {
	for range r.Handlers {
		go r.Listen()
	}
}

func (r *RPCServer) Listen() {
	for {
		r.subscribe()
		time.Sleep(time.Duration(r.reconnectDelay) * time.Second)
	}
}

func (r *RPCServer) subscribe() {
	conn := r.client.GetConnection()

	if conn == nil {
		log.Error().Msg("received an empty connection")
		return
	}

	ch, errChannel := conn.Channel()
	if errChannel != nil {
		log.Error().Err(errChannel).Msg("could not initialize channel")
		return
	}

	if err := ch.Qos(1, 0, false); err != nil {
		log.Error().Err(err).Msg("Failed to set QoS")
		return
	}

	msgs, errConsume := ch.Consume(
		r.queueOptions.Name,
		r.client.GetName(),
		false,
		false,
		false,
		false,
		nil,
	)

	if errConsume != nil {
		log.Error().Err(errConsume).Msg("could not subscribe to queue")
	}

	for msg := range msgs {
		r.Handle(msg, ch)
	}
}

func (r *RPCServer) Handle(msg amqp.Delivery, ch *amqp.Channel) {
	ctx := context.Background()

	command := &Request{
		body:    msg.Body,
		headers: msg.Headers,
	}

	var route string
	if typeFromHeader, ok := command.headers[headerRouteKey]; ok {
		route = typeFromHeader.(string)
	}

	for _, h := range r.Handlers {
		if h.name != route {
			continue
		}

		response := h.handle(ctx, command)
		body, err := response.GetBody()
		if err != nil {
			_ = msg.Nack(false, false)
			return
		}

		if msg.ReplyTo != "" {
			if err = ch.PublishWithContext(
				ctx,
				"",
				msg.ReplyTo,
				false,
				false,
				amqp.Publishing{
					Headers:       response.GetHeaders(),
					CorrelationId: msg.CorrelationId,
					Body:          body,
				},
			); err != nil {
				_ = msg.Nack(false, false)
				return
			}
		}

		_ = msg.Ack(false)
		return
	}

	_ = msg.Nack(false, false)
}

func (r *RPCServer) AddHandler(name string, handle Handle) error {
	for _, h := range r.Handlers {
		if h.name == name {
			return ErrHandlerNameBusy
		}
	}

	r.Handlers = append(r.Handlers, &Handler{
		client: r.client,
		name:   name,
		handle: handle,
	})

	return nil
}

func (r *RPCServer) initQueue() error {
	if !r.client.GetStatus() {
		return ErrConnectionClose
	}

	ch := r.client.GetChannel()

	if ch == nil {
		return ErrConnectionClose
	}

	return declareQueueAndBind(ch, r.queueOptions, []*BindOptions{})
}
