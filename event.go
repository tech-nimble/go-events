package events

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Event struct {
	ID         string
	EntityId   string
	EntityName string
	Published  bool
	Payload    interface{}
	Headers    map[string]interface{}
	Exchange   string
	RoutingKey string
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

func NewEvent(payload interface{}, headers map[string]interface{}, entityId, entityName, routingKey, exchange string) *Event {
	return &Event{
		ID:         uuid.NewString(),
		Payload:    payload,
		Headers:    headers,
		Published:  false,
		EntityId:   entityId,
		EntityName: entityName,
		RoutingKey: routingKey,
		Exchange:   exchange,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Time{},
	}
}

func (e *Event) Sent() {
	e.Published = true
	e.UpdatedAt = time.Now()
}

func (e *Event) GetHeaders() amqp.Table {
	return e.Headers
}

func (e *Event) SetHeaders(headers map[string]interface{}) {
	e.Headers = headers
}

func (e *Event) GetBody() ([]byte, error) {
	return json.Marshal(e.Payload)
}

func (e *Event) GetDeliveryMode() DeliveryMode {
	return DeliveryMode(amqp.Persistent)
}

func (e *Event) GetRoutingKey() string {
	return e.RoutingKey
}
