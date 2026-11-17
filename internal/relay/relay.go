package relay

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Publisher interface {
	Publish(ctx context.Context, messageID uuid.UUID, payload []byte) error
}

type Relay struct {
	pool    *pgxpool.Pool
	publish Publisher
}

func New(pool *pgxpool.Pool, publish Publisher) *Relay {
	return &Relay{pool: pool, publish: publish}
}

// Tick claims up to batch unpublished outbox rows with
// SELECT ... FOR UPDATE SKIP LOCKED, publishes each one, then marks them
// sent. Publish and the mark live in separate transactions so a crash
// after publish and before the mark can produce a broker-level duplicate.
func (r *Relay) Tick(ctx context.Context, batch int) (int, error) {
	if batch <= 0 {
		batch = 100
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, `
		SELECT id, message_id, payload
		FROM outbox
		WHERE published_at IS NULL
		ORDER BY id
		LIMIT $1
		FOR UPDATE SKIP LOCKED`, batch)
	if err != nil {
		return 0, fmt.Errorf("select outbox: %w", err)
	}

	type row struct {
		id      int64
		msgID   uuid.UUID
		payload []byte
	}
	var claimed []row
	for rows.Next() {
		var rec row
		if err := rows.Scan(&rec.id, &rec.msgID, &rec.payload); err != nil {
			rows.Close()
			return 0, fmt.Errorf("scan: %w", err)
		}
		claimed = append(claimed, rec)
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
	for _, rec := range claimed {
		if err := r.publish.Publish(ctx, rec.msgID, rec.payload); err != nil {
			return 0, err
		}
		ids = append(ids, rec.id)
	}

	_, err = tx.Exec(ctx, `
		UPDATE outbox
		SET published_at = now(),
		    attempts = attempts + 1
		WHERE id = ANY($1)`, ids)
	if err != nil {
		return 0, fmt.Errorf("mark sent: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit: %w", err)
	}
	return len(claimed), nil
}
