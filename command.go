// SPDX-FileCopyrightText: 2025 Nimble Tech
// SPDX-License-Identifier: MIT

package events

type Command interface {
	GetBody() ([]byte, error)
	GetExchangeName() string
	GetCommandName() string
	GetHeaders() map[string]any
	SetHeaders(map[string]any)
}

type Response interface {
	GetBody() ([]byte, error)
	GetHeaders() map[string]any
	SetHeaders(map[string]any)
}
