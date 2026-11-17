package itest

import (
	"context"
	"fmt"
	"net"
	"os"
	"sync"
	"testing"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/federiconbaez/cortex9-outbox-relay-go/internal/schema"
)

var testPool *pgxpool.Pool

func TestMain(m *testing.M) {
	ctx := context.Background()
	var ep *embeddedpostgres.EmbeddedPostgres
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		port, err := freePort()
		if err != nil {
			fmt.Fprintf(os.Stderr, "puerto libre: %v\n", err)
			os.Exit(1)
		}
		ep = embeddedpostgres.NewDatabase(embeddedpostgres.DefaultConfig().
			Username("postgres").
			Password("postgres").
			Database("outbox").
			Port(uint32(port)).
			Locale("C"))
		if err := ep.Start(); err != nil {
			fmt.Fprintf(os.Stderr, "embedded-postgres: %v\n", err)
			os.Exit(1)
		}
		url = fmt.Sprintf("postgres://postgres:postgres@127.0.0.1:%d/outbox?sslmode=disable", port)
	}

	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		fmt.Fprintf(os.Stderr, "parse url: %v\n", err)
		os.Exit(1)
	}
	cfg.MaxConns = 16
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "conectar: %v\n", err)
		if ep != nil {
			_ = ep.Stop()
		}
		os.Exit(1)
	}
	if err := schema.Apply(ctx, pool); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		pool.Close()
		if ep != nil {
			_ = ep.Stop()
		}
		os.Exit(1)
	}
	testPool = pool
	testDatabaseURL = url

	code := m.Run()

	pool.Close()
	if ep != nil {
		_ = ep.Stop()
	}
	os.Exit(code)
}

var testDatabaseURL string

func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

var resetMu sync.Mutex

func reset(t *testing.T) {
	t.Helper()
	resetMu.Lock()
	defer resetMu.Unlock()
	ctx := context.Background()
	_, err := testPool.Exec(ctx, `
		TRUNCATE orders, outbox, broker_messages, processed_messages, order_effects
		RESTART IDENTITY CASCADE`)
	if err != nil {
		t.Fatalf("truncate: %v", err)
	}
}

type counts struct {
	Orders          int64
	Outbox          int64
	Unpublished     int64
	Broker          int64
	BrokerDistinct  int64
	Unconsumed      int64
	Effects         int64
	EffectsDistinct int64
	Processed       int64
}

func readCounts(t *testing.T) counts {
	t.Helper()
	var c counts
	err := testPool.QueryRow(context.Background(), `
		SELECT
			(SELECT count(*) FROM orders),
			(SELECT count(*) FROM outbox),
			(SELECT count(*) FROM outbox WHERE published_at IS NULL),
			(SELECT count(*) FROM broker_messages),
			(SELECT count(DISTINCT message_id) FROM broker_messages),
			(SELECT count(*) FROM broker_messages WHERE consumed_at IS NULL),
			(SELECT count(*) FROM order_effects),
			(SELECT count(DISTINCT message_id) FROM order_effects),
			(SELECT count(*) FROM processed_messages)`).
		Scan(&c.Orders, &c.Outbox, &c.Unpublished, &c.Broker, &c.BrokerDistinct,
			&c.Unconsumed, &c.Effects, &c.EffectsDistinct, &c.Processed)
	if err != nil {
		t.Fatalf("counts: %v", err)
	}
	return c
}
