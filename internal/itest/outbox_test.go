package itest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/federiconbaez/cortex9-outbox-relay-go/internal/broker"
	"github.com/federiconbaez/cortex9-outbox-relay-go/internal/consumer"
	"github.com/federiconbaez/cortex9-outbox-relay-go/internal/relay"
	"github.com/federiconbaez/cortex9-outbox-relay-go/internal/store"
)

func TestCreateOrderWritesBusinessAndOutboxAtomically(t *testing.T) {
	reset(t)
	ctx := context.Background()
	st := store.New(testPool)

	o, err := st.CreateOrder(ctx, "alice", 1500)
	if err != nil {
		t.Fatal(err)
	}
	if o.ID == 0 || o.MessageID == uuid.Nil {
		t.Fatalf("pedido incompleto: %+v", o)
	}
	c := readCounts(t)
	if c.Orders != 1 || c.Outbox != 1 || c.Unpublished != 1 {
		t.Fatalf("despues de CreateOrder: %+v", c)
	}

	tx, err := testPool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.InsertOrderWithOutbox(ctx, tx, "bob", 2000); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	c = readCounts(t)
	if c.Orders != 1 || c.Outbox != 1 {
		t.Fatalf("el rollback debio dejar solo el primer pedido: %+v", c)
	}
}

func TestRelayPublishesAndMarksSent(t *testing.T) {
	reset(t)
	ctx := context.Background()
	st := store.New(testPool)
	if _, err := st.CreateOrder(ctx, "alice", 1500); err != nil {
		t.Fatal(err)
	}
	r := relay.New(testPool, broker.New(testPool))
	n, err := r.Tick(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("tick=%d, quiero 1", n)
	}
	c := readCounts(t)
	if c.Unpublished != 0 || c.Broker != 1 || c.BrokerDistinct != 1 {
		t.Fatalf("despues del relay: %+v", c)
	}
	n, err = r.Tick(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("segundo tick=%d, quiero 0", n)
	}
}

func TestRelaySkipLockedDoesNotClaimHeldRows(t *testing.T) {
	reset(t)
	ctx := context.Background()
	if _, err := store.New(testPool).CreateOrder(ctx, "alice", 1500); err != nil {
		t.Fatal(err)
	}

	hold, err := testPool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = hold.Rollback(ctx) }()
	var id int64
	if err := hold.QueryRow(ctx, `
		SELECT id FROM outbox
		WHERE published_at IS NULL
		FOR UPDATE SKIP LOCKED`).Scan(&id); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		n, err := relay.New(testPool, broker.New(testPool)).Tick(ctx, 10)
		if err != nil {
			done <- err
			return
		}
		if n != 0 {
			done <- fmt.Errorf("tick tomo %d filas bloqueadas", n)
			return
		}
		done <- nil
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("el segundo relay se bloqueo en vez de usar SKIP LOCKED")
	}

	c := readCounts(t)
	if c.Unpublished != 1 || c.Broker != 0 {
		t.Fatalf("la fila retenida no debio publicarse: %+v", c)
	}
}

func TestConsumerIsIdempotentByMessageID(t *testing.T) {
	reset(t)
	ctx := context.Background()
	st := store.New(testPool)
	o, err := st.CreateOrder(ctx, "alice", 1500)
	if err != nil {
		t.Fatal(err)
	}
	r := relay.New(testPool, broker.New(testPool))
	if _, err := r.Tick(ctx, 10); err != nil {
		t.Fatal(err)
	}
	// Replay the same message id at the broker (at-least-once).
	if _, err := testPool.Exec(ctx,
		`INSERT INTO broker_messages (message_id, payload)
		 SELECT message_id, payload FROM broker_messages LIMIT 1`); err != nil {
		t.Fatal(err)
	}

	cons := consumer.New(testPool)
	n, err := cons.Tick(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("tick=%d, quiero 2 filas del broker", n)
	}
	c := readCounts(t)
	if c.Effects != 1 || c.EffectsDistinct != 1 || c.Processed != 1 || c.Unconsumed != 0 {
		t.Fatalf("efecto duplicado o mensaje sin consumir: %+v message_id=%s", c, o.MessageID)
	}
}

func TestRelayAndConsumerHappyPath(t *testing.T) {
	reset(t)
	ctx := context.Background()
	const n = 500
	if err := store.New(testPool).CreateOrders(ctx, n); err != nil {
		t.Fatal(err)
	}
	r := relay.New(testPool, broker.New(testPool))
	cons := consumer.New(testPool)
	for {
		got, err := r.Tick(ctx, 100)
		if err != nil {
			t.Fatal(err)
		}
		if got == 0 {
			break
		}
	}
	for {
		got, err := cons.Tick(ctx, 100)
		if err != nil {
			t.Fatal(err)
		}
		if got == 0 {
			break
		}
	}
	c := readCounts(t)
	if c.Orders != n || c.Unpublished != 0 || c.Unconsumed != 0 || c.Effects != n || c.Processed != n {
		t.Fatalf("camino feliz incompleto: %+v", c)
	}
}
