package consumer

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/federiconbaez/cortex9-outbox-relay-go/internal/store"
)

type Consumer struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Consumer {
	return &Consumer{pool: pool}
}

// Tick claims unconsumed broker rows with FOR UPDATE SKIP LOCKED and, in
// the same transaction, records the message id in processed_messages and
// applies the side effect. A replay hits ON CONFLICT DO NOTHING and does
// not insert a second effect.
func (c *Consumer) Tick(ctx context.Context, batch int) (int, error) {
	if batch <= 0 {
		batch = 100
	}

	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, `
		SELECT id, message_id, payload
		FROM broker_messages
		WHERE consumed_at IS NULL
		ORDER BY id
		LIMIT $1
		FOR UPDATE SKIP LOCKED`, batch)
	if err != nil {
		return 0, fmt.Errorf("select broker: %w", err)
	}

	type rec struct {
		id      int64
		msgID   uuid.UUID
		payload []byte
	}
	var claimed []rec
	for rows.Next() {
		var r rec
		if err := rows.Scan(&r.id, &r.msgID, &r.payload); err != nil {
			rows.Close()
			return 0, fmt.Errorf("scan: %w", err)
		}
		claimed = append(claimed, r)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, fmt.Errorf("rows: %w", err)
	}
	rows.Close()

	if len(claimed) == 0 {
		return 0, nil
	}

	ids := make([]int64, 0, len(claimed))
	for _, r := range claimed {
		tag, err := tx.Exec(ctx,
			`INSERT INTO processed_messages (message_id) VALUES ($1) ON CONFLICT DO NOTHING`,
			r.msgID,
		)
		if err != nil {
			return 0, fmt.Errorf("processed_messages: %w", err)
		}
		if tag.RowsAffected() == 1 {
			var p store.Payload
			if err := json.Unmarshal(r.payload, &p); err != nil {
				return 0, fmt.Errorf("payload: %w", err)
			}
			_, err = tx.Exec(ctx, `
				INSERT INTO order_effects (message_id, order_id, customer_id, amount_cents)
				VALUES ($1, $2, $3, $4)`,
				r.msgID, p.OrderID, p.CustomerID, p.AmountCents,
			)
			if err != nil {
				return 0, fmt.Errorf("order_effects: %w", err)
			}
		}
		ids = append(ids, r.id)
	}

	_, err = tx.Exec(ctx,
		`UPDATE broker_messages SET consumed_at = now() WHERE id = ANY($1)`,
		ids,
	)
	if err != nil {
		return 0, fmt.Errorf("mark consumed: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit: %w", err)
	}
	return len(claimed), nil
}
