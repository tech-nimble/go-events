// SPDX-FileCopyrightText: 2025 Nimble Tech
// SPDX-License-Identifier: MIT

package amqp_v2

import (
	"github.com/tech-nimble/go-events"
	"github.com/tech-nimble/go-tools/repositories"
)

func InitializeEventRepository(rep *repositories.Repositories) *events.DBRepository {
	return events.NewDBRepository(rep, "events")
}

func InitializeEventBus(client events.Publisher, rep *events.DBRepository) *events.EventBus {
	return events.NewEventBus(client, rep)
}
