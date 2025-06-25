package rabbitmq

import (
	"context"
	"errors"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/tech-nimble/go-events"
)

type Response struct {
	body    []byte
	headers map[string]interface{}
}

func (r Response) GetBody() ([]byte, error) {
	return r.body, nil
}

func (r Response) GetHeaders() map[string]interface{} {
	return r.headers
}

func (r Response) SetHeaders(m map[string]interface{}) {
	panic("implement me")
}

type RPCClient struct {
	client   *RabbitMQ
	exchange *Exchange
}

func NewRPCClient(client *RabbitMQ, exchangeName string) *RPCClient {
	return &RPCClient{
		client: client,
		exchange: &Exchange{
			client: client,
			Name:   exchangeName,
			Options: ExchangeOptions{
				Name: exchangeName,
			},
		},
	}
}

func (r *RPCClient) Do(ctx context.Context, command events.Command) (events.Response, error) {
	conn := r.client.GetConnection()

	if conn == nil {
		return nil, ErrConnectionClose
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	defer ch.Close()

	err = ch.Qos(1, 0, false)
	if err != nil {
		return nil, err
	}

	q, err := ch.QueueDeclare("", false, false, true, false, nil)
	if err != nil {
		return nil, err
	}

	msgs, err := ch.Consume(
		q.Name,
		r.client.GetName(),
		true,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		return nil, err
	}

	body, err := command.GetBody()
	if err != nil {
		return nil, err
	}

	correlationID := uuid.New().String()

	_ = ch.PublishWithContext(
		ctx,
		command.GetExchangeName(),
		command.GetCommandName(),
		false,
		false,
		amqp.Publishing{
			Headers:       command.GetHeaders(),
			CorrelationId: correlationID,
			ReplyTo:       q.Name,
			Body:          body,
		},
	)

	select {
	case msg := <-msgs:
		if correlationID == msg.CorrelationId {
			return &Response{
				body:    msg.Body,
				headers: msg.Headers,
			}, nil
		}
	case <-ctx.Done():
		return nil, errors.New("timeout")
	}

	return nil, nil
}
