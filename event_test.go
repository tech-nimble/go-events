// SPDX-FileCopyrightText: 2025 Nimble Tech
// SPDX-License-Identifier: MIT

package events

import "testing"

func TestNewEvent(t *testing.T) {
	t.Parallel()

	e := NewEvent(map[string]string{"k": "v"}, nil, "42", "order", "order.created", "orders")

	if e.ID == "" {
		t.Fatal("expected generated id")
	}
	if e.EntityID != "42" || e.EntityName != "order" {
		t.Fatalf("unexpected entity fields: %+v", e)
	}
	if e.Published {
		t.Fatal("new event must not be published")
	}
}

func TestEventSent(t *testing.T) {
	t.Parallel()

	e := NewEvent(nil, nil, "1", "order", "order.created", "orders")
	e.Sent()

	if !e.Published {
		t.Fatal("expected event to be marked published")
	}
	if e.UpdatedAt.IsZero() {
		t.Fatal("expected UpdatedAt to be set")
	}
}
