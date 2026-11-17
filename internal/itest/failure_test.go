package itest

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/federiconbaez/cortex9-outbox-relay-go/internal/store"
)

const failureN = 10000

func TestFailureKillMidRun(t *testing.T) {
	reset(t)
	ctx := context.Background()
	if err := store.New(testPool).CreateOrders(ctx, failureN); err != nil {
		t.Fatal(err)
	}

	bin := buildWorker(t)
	const plannedKills = 8
	start := time.Now()
	kills := 0

	for i := 0; i < plannedKills; i++ {
		c := readCounts(t)
		if drained(c, failureN) {
			break
		}
		relayCmd := startWorker(t, bin, "relay")
		consCmd := startWorker(t, bin, "consumer")
		// Wait until some work has happened, or a short bound if already in flight.
		deadline := time.Now().Add(400 * time.Millisecond)
		for time.Now().Before(deadline) {
			c = readCounts(t)
			if c.Broker > 0 || c.Effects > 0 || drained(c, failureN) {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		time.Sleep(80 * time.Millisecond)
		kills += killProcess(t, relayCmd)
		kills += killProcess(t, consCmd)
	}

	// Final pass: run until drained or timeout.
	relayCmd := startWorker(t, bin, "relay")
	consCmd := startWorker(t, bin, "consumer")
	defer func() {
		_ = signalTerm(relayCmd)
		_ = signalTerm(consCmd)
		_, _ = relayCmd.Process.Wait()
		_, _ = consCmd.Process.Wait()
	}()

	var c counts
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		c = readCounts(t)
		if drained(c, failureN) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	elapsed := time.Since(start)
	if !drained(c, failureN) {
		t.Fatalf("no termino en %s: %+v kills=%d", elapsed, c, kills)
	}

	lost := failureN - c.EffectsDistinct
	dupEffects := c.Effects - c.EffectsDistinct
	dupBroker := c.Broker - c.BrokerDistinct
	if lost != 0 || dupEffects != 0 || c.Effects != failureN || c.Processed != failureN {
		t.Fatalf("invariantes rotas: lost=%d dup_effects=%d counts=%+v", lost, dupEffects, c)
	}

	msg := fmt.Sprintf(
		"FAILURE_TEST n=%d kills=%d broker_rows=%d broker_distinct=%d broker_dups=%d effects=%d effects_distinct=%d lost=%d dup_effects=%d duration=%s",
		failureN, kills, c.Broker, c.BrokerDistinct, dupBroker, c.Effects, c.EffectsDistinct, lost, dupEffects, elapsed.Round(time.Millisecond),
	)
	t.Log(msg)
	fmt.Println(msg)
}

func TestThroughputCleanRun(t *testing.T) {
	reset(t)
	ctx := context.Background()
	const n = 10000
	if err := store.New(testPool).CreateOrders(ctx, n); err != nil {
		t.Fatal(err)
	}
	bin := buildWorker(t)
	start := time.Now()
	relayCmd := startWorker(t, bin, "relay")
	consCmd := startWorker(t, bin, "consumer")
	defer func() {
		_ = signalTerm(relayCmd)
		_ = signalTerm(consCmd)
		_, _ = relayCmd.Process.Wait()
		_, _ = consCmd.Process.Wait()
	}()

	var c counts
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		c = readCounts(t)
		if drained(c, n) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	elapsed := time.Since(start)
	if !drained(c, n) {
		t.Fatalf("throughput no termino en %s: %+v", elapsed, c)
	}
	perSec := float64(n) / elapsed.Seconds()
	msg := fmt.Sprintf("THROUGHPUT_TEST n=%d duration=%s throughput=%.0f msg/s broker_rows=%d broker_dups=%d",
		n, elapsed.Round(time.Millisecond), perSec, c.Broker, c.Broker-c.BrokerDistinct)
	t.Log(msg)
	fmt.Println(msg)
}

func drained(c counts, n int) bool {
	want := int64(n)
	return c.Orders == want && c.Outbox == want &&
		c.Unpublished == 0 && c.Unconsumed == 0 &&
		c.Effects == want && c.EffectsDistinct == want &&
		c.Processed == want
}

func buildWorker(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
	bin := filepath.Join(t.TempDir(), "worker")
	cmd := exec.Command("go", "build", "-o", bin, "./cmd/worker")
	cmd.Dir = root
	cmd.Env = os.Environ()
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("go build ./cmd/worker: %v\n%s", err, out)
	}
	return bin
}

func startWorker(t *testing.T, bin, mode string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(bin, "-database-url", testDatabaseURL, "-mode", mode, "-batch", "50")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start %s: %v", mode, err)
	}
	return cmd
}

func killProcess(t *testing.T, cmd *exec.Cmd) int {
	t.Helper()
	if cmd.Process == nil {
		return 0
	}
	if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Logf("SIGKILL: %v", err)
	}
	_, _ = cmd.Process.Wait()
	return 1
}

func signalTerm(cmd *exec.Cmd) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	return cmd.Process.Signal(syscall.SIGTERM)
}
