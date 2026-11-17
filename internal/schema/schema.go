package schema

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
)

const SQL = `
CREATE TABLE IF NOT EXISTS orders (
    id           BIGSERIAL PRIMARY KEY,
    customer_id  TEXT        NOT NULL,
    amount_cents BIGINT      NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS outbox (
    id             BIGSERIAL PRIMARY KEY,
    message_id     UUID        NOT NULL UNIQUE,
    aggregate_type TEXT        NOT NULL,
    aggregate_id   BIGINT      NOT NULL,
    payload        JSONB       NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at   TIMESTAMPTZ,
    attempts       INT         NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS outbox_unpublished_idx
    ON outbox (id)
    WHERE published_at IS NULL;

CREATE TABLE IF NOT EXISTS broker_messages (
    id           BIGSERIAL PRIMARY KEY,
    message_id   UUID        NOT NULL,
    payload      JSONB       NOT NULL,
    published_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    consumed_at  TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS broker_unconsumed_idx
    ON broker_messages (id)
    WHERE consumed_at IS NULL;

CREATE TABLE IF NOT EXISTS processed_messages (
    message_id   UUID PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS order_effects (
    id           BIGSERIAL PRIMARY KEY,
    message_id   UUID        NOT NULL UNIQUE,
    order_id     BIGINT      NOT NULL,
    customer_id  TEXT        NOT NULL,
    amount_cents BIGINT      NOT NULL,
    applied_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
`

// Execer is satisfied by *pgxpool.Pool and pgx.Tx.
type Execer interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

// Apply creates the schema if it does not already exist.
func Apply(ctx context.Context, db Execer) error {
	if _, err := db.Exec(ctx, SQL); err != nil {
		return fmt.Errorf("aplicar esquema: %w", err)
	}
	return nil
}
