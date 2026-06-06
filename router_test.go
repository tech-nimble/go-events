// SPDX-FileCopyrightText: 2025 Nimble Tech
// SPDX-License-Identifier: MIT

package events

import (
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestHeaderRouterMatch(t *testing.T) {
	t.Parallel()

	router := NewHeaderRouter()
	msg := amqp.Delivery{Headers: amqp.Table{"type": "order.created"}}

	if !router.Match(msg, "order.created") {
		t.Fatal("expected match on header type")
	}

	if router.Match(msg, "order.updated") {
		t.Fatal("did not expect match on a different route")
	}
}

func TestHeaderRouterRegexpMatcher(t *testing.T) {
	t.Parallel()

	router := NewHeaderRouter(WithHeaderRouterRegexpMatcher())
	msg := amqp.Delivery{Headers: amqp.Table{"type": "order.created"}}

	if !router.Match(msg, "^order\\.") {
		t.Fatal("expected regexp match")
	}
}

func TestRouterKeyRouterMatch(t *testing.T) {
	t.Parallel()

	router := NewRouterKeyRouter()
	msg := amqp.Delivery{RoutingKey: "payments.initiated"}

	if !router.Match(msg, "payments.initiated") {
		t.Fatal("expected match on routing key")
	}

	if router.Match(msg, "payments.failed") {
		t.Fatal("did not expect match on a different routing key")
	}
}
