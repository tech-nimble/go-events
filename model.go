package events

import (
	"database/sql"
	"encoding/json"

	"github.com/jackc/pgtype"
)

type Model struct {
	ID         string
	EntityId   string
	EntityName string
	Published  bool
	Payload    pgtype.JSONB
	Headers    pgtype.JSONB
	Exchange   string
	RoutingKey string
	CreatedAt  pgtype.Timestamp
	UpdatedAt  sql.NullTime
}

func NewModel(entity *Event) (*Model, error) {
	var payloadPg pgtype.JSONB
	if err := payloadPg.Set(entity.Payload); err != nil {
		return nil, err
	}

	var headersPg pgtype.JSONB
	if err := headersPg.Set(entity.Headers); err != nil {
		return nil, err
	}

	createdAt := pgtype.Timestamp{}
	if err := createdAt.Set(entity.CreatedAt); err != nil {
		return nil, err
	}

	return &Model{
		ID:         entity.ID,
		Payload:    payloadPg,
		Headers:    headersPg,
		Published:  entity.Published,
		EntityId:   entity.EntityId,
		EntityName: entity.EntityName,
		Exchange:   entity.Exchange,
		RoutingKey: entity.RoutingKey,
		CreatedAt:  createdAt,
		UpdatedAt:  sql.NullTime{Time: entity.UpdatedAt, Valid: !entity.UpdatedAt.IsZero()},
	}, nil
}

func (e *Model) GetEntity() (*Event, error) {
	var headers map[string]interface{}
	if err := json.Unmarshal(e.Headers.Bytes, &headers); err != nil {
		return nil, err
	}

	return &Event{
		ID:         e.ID,
		Payload:    e.Payload,
		Headers:    headers,
		Published:  e.Published,
		EntityId:   e.EntityId,
		EntityName: e.EntityName,
		Exchange:   e.Exchange,
		RoutingKey: e.RoutingKey,
		CreatedAt:  e.CreatedAt.Time,
		UpdatedAt:  e.UpdatedAt.Time,
	}, nil
}
