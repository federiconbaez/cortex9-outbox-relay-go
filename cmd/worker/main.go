package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/federiconbaez/cortex9-outbox-relay-go/internal/broker"
	"github.com/federiconbaez/cortex9-outbox-relay-go/internal/consumer"
	"github.com/federiconbaez/cortex9-outbox-relay-go/internal/relay"
	"github.com/federiconbaez/cortex9-outbox-relay-go/internal/schema"
	"github.com/federiconbaez/cortex9-outbox-relay-go/internal/store"
)

func main() {
	dbURL := flag.String("database-url", os.Getenv("DATABASE_URL"), "URL de Postgres")
	mode := flag.String("mode", "run", "produce|relay|consumer|run")
	n := flag.Int("n", 0, "mensajes a producir (mode=produce)")
	batch := flag.Int("batch", 100, "tamaño de lote")
	flag.Parse()

	if *dbURL == "" {
		log.Fatal("hace falta -database-url o DATABASE_URL")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	cfg, err := pgxpool.ParseConfig(*dbURL)
	if err != nil {
		log.Fatalf("database-url: %v", err)
	}
	cfg.MaxConns = 8
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		log.Fatalf("conectar: %v", err)
	}
	defer pool.Close()

	if err := schema.Apply(ctx, pool); err != nil {
		log.Fatal(err)
	}

	switch *mode {
	case "produce":
		if err := store.New(pool).CreateOrders(ctx, *n); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("produced %d\n", *n)
	case "relay":
		r := relay.New(pool, broker.New(pool))
		runLoop(ctx, *batch, r.Tick)
	case "consumer":
		c := consumer.New(pool)
		runLoop(ctx, *batch, c.Tick)
	case "run":
		r := relay.New(pool, broker.New(pool))
		c := consumer.New(pool)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			runLoop(ctx, *batch, r.Tick)
		}()
		go func() {
			defer wg.Done()
			runLoop(ctx, *batch, c.Tick)
		}()
		wg.Wait()
	default:
		log.Fatalf("mode desconocido: %s", *mode)
	}
}

func runLoop(ctx context.Context, batch int, tick func(context.Context, int) (int, error)) {
	for {
		if ctx.Err() != nil {
			return
		}
		n, err := tick(ctx, batch)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("tick: %v", err)
			time.Sleep(20 * time.Millisecond)
			continue
		}
		if n == 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(3 * time.Millisecond):
			}
		}
	}
}
