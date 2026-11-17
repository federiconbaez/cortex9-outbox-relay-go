package store

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Order struct {
	ID          int64
	CustomerID  string
	AmountCents int64
	MessageID   uuid.UUID
}

type Payload struct {
	OrderID     int64  `json:"order_id"`
	CustomerID  string `json:"customer_id"`
	AmountCents int64  `json:"amount_cents"`
}

type Store struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// CreateOrder writes the business row and the outbox row in the same transaction.
func (s *Store) CreateOrder(ctx context.Context, customerID string, amountCents int64) (Order, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Order{}, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	o, err := InsertOrderWithOutbox(ctx, tx, customerID, amountCents)
	if err != nil {
		return Order{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Order{}, fmt.Errorf("commit: %w", err)
	}
	return o, nil
}

// CreateOrders writes n orders, each paired with its outbox row, in chunks
// that still keep every pair inside the same transaction.
func (s *Store) CreateOrders(ctx context.Context, n int) error {
	const chunk = 200
	for i := 0; i < n; i += chunk {
		count := chunk
		if i+count > n {
			count = n - i
		}
		if err := s.createChunk(ctx, i, count); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) createChunk(ctx context.Context, start, count int) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	for i := 0; i < count; i++ {
		customer := fmt.Sprintf("c-%d", start+i)
		if _, err := InsertOrderWithOutbox(ctx, tx, customer, int64(100+start+i)); err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// InsertOrderWithOutbox performs the two inserts on an existing transaction.
func InsertOrderWithOutbox(ctx context.Context, tx pgx.Tx, customerID string, amountCents int64) (Order, error) {
	var id int64
	err := tx.QueryRow(ctx,
		`INSERT INTO orders (customer_id, amount_cents) VALUES ($1, $2) RETURNING id`,
		customerID, amountCents,
	).Scan(&id)
	if err != nil {
		return Order{}, fmt.Errorf("insert order: %w", err)
	}

	msgID := uuid.New()
	payload, err := json.Marshal(Payload{
		OrderID:     id,
		CustomerID:  customerID,
		AmountCents: amountCents,
	})
	if err != nil {
		return Order{}, fmt.Errorf("payload: %w", err)
	}
	_, err = tx.Exec(ctx,
		`INSERT INTO outbox (message_id, aggregate_type, aggregate_id, payload)
		 VALUES ($1, 'order', $2, $3)`,
		msgID, id, payload,
	)
	if err != nil {
		return Order{}, fmt.Errorf("insert outbox: %w", err)
	}
	return Order{ID: id, CustomerID: customerID, AmountCents: amountCents, MessageID: msgID}, nil
}
