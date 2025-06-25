package events

import (
	"context"

	"github.com/tech-nimble/go-tools/helpers/jaeger"
)

type EventBus struct {
	client     Publisher
	repository Repository
}

func NewEventBus(client Publisher, repository Repository) *EventBus {
	return &EventBus{
		client:     client,
		repository: repository,
	}
}

func (e *EventBus) Send(ctx context.Context, event *Event) error {
	span, _ := jaeger.StartSpanFromContext(ctx, "EventBus::Send")
	defer span.Finish()

	if err := jaeger.InjectSpanContextToAmqp(span, event); err != nil {
		return err
	}

	if err := e.client.Publish(event); err != nil {
		return err
	}

	event.Sent()
	return e.repository.Update(ctx, event)
}
