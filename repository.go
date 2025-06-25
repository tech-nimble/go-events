package events

import (
	"context"
	"fmt"

	"github.com/tech-nimble/go-tools/helpers/jaeger"
	"github.com/tech-nimble/go-tools/repositories"
)

type Repository interface {
	Create(ctx context.Context, event *Event) error
	Update(ctx context.Context, event *Event) error
}

type DBRepository struct {
	*repositories.Repositories
	tableName string
}

func NewDBRepository(rep *repositories.Repositories, tableName string) *DBRepository {
	return &DBRepository{
		Repositories: rep,
		tableName:    tableName,
	}
}

func (e *DBRepository) Create(ctx context.Context, entity *Event) error {
	span, ctx := jaeger.StartSpanFromContext(ctx, "DB 'events':Create")
	defer span.Finish()

	db := e.GetConnect(ctx)

	query := fmt.Sprintf(
		"insert into %s(id, entity_id, entity_name, payload, headers, published, created_at, updated_at, exchange, routing_key)"+
			" values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
		e.tableName,
	)

	model, err := NewModel(entity)
	if err != nil {
		return err
	}

	_, err = db.Exec(
		ctx,
		query,
		model.ID,
		model.EntityId,
		model.EntityName,
		model.Payload,
		model.Headers,
		model.Published,
		model.CreatedAt,
		model.UpdatedAt,
		model.Exchange,
		model.RoutingKey,
	)
	if err != nil {
		return err
	}

	return nil
}

func (e *DBRepository) Update(ctx context.Context, entity *Event) error {
	span, ctx := jaeger.StartSpanFromContext(ctx, "DB 'events':Update")
	defer span.Finish()

	db := e.DB

	query := fmt.Sprintf(
		"update %s set published = $1, updated_at = $2 where id = $3",
		e.tableName,
	)

	model, err := NewModel(entity)
	if err != nil {
		return err
	}

	_, err = db.Exec(
		ctx,
		query,
		model.Published,
		model.UpdatedAt,
		model.ID,
	)

	if err != nil {
		return err
	}

	return nil
}
