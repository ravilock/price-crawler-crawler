package scheduler

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestDomainBudgetConcurrencyPacingAndCooldown(t *testing.T) {
	budget := NewBudget(2)
	ctx := context.Background()
	release, err := budget.Acquire(ctx, "store", 20*time.Millisecond, 1)
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan time.Time, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		done, err := budget.Acquire(ctx, "store", 20*time.Millisecond, 1)
		if err == nil {
			entered <- time.Now()
			done()
		}
	}()
	select {
	case <-entered:
		t.Fatal("exceeded per-domain concurrency")
	case <-time.After(30 * time.Millisecond):
	}
	release()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("waiting request did not proceed")
	}
	wg.Wait()
	budget.Cooldown("store", time.Now().Add(60*time.Millisecond))
	start := time.Now()
	done, err := budget.Acquire(ctx, "store", 20*time.Millisecond, 1)
	if err != nil {
		t.Fatal(err)
	}
	done()
	if time.Since(start) < 50*time.Millisecond {
		t.Fatal("cooldown bypassed")
	}
}
func TestBudgetCancellationAndRelease(t *testing.T) {
	b := NewBudget(1)
	release, err := b.Acquire(context.Background(), "a", time.Second, 1)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err = b.Acquire(ctx, "b", time.Second, 1); err == nil {
		t.Fatal("worker limit bypassed")
	}
	release()
	release()
	done, err := b.Acquire(context.Background(), "b", time.Second, 1)
	if err != nil {
		t.Fatal(err)
	}
	done()
}
