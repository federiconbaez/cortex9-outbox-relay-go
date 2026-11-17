package broker

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PG is a Postgres table used as a message broker.
// Duplicate message_id values are allowed: the relay publishes and then
// marks the outbox row in a separate transaction, so a crash between those
// two commits can replay the same message (at-least-once).
type PG struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *PG {
	return &PG{pool: pool}
}

func (b *PG) Publish(ctx context.Context, messageID uuid.UUID, payload []byte) error {
	_, err := b.pool.Exec(ctx,
		`INSERT INTO broker_messages (message_id, payload) VALUES ($1, $2)`,
		messageID, payload,
	)
	if err != nil {
		return fmt.Errorf("publish: %w", err)
	}
	return nil
}
