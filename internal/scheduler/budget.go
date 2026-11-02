// Package scheduler owns collection scheduling and source request budgets.
package scheduler

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"
)

type domainState struct {
	active         int
	next, cooldown time.Time
}

// Budget controls all outbound request classes through a shared worker limit and
// per-domain concurrency, pacing and cooldown. It is safe for concurrent use.
type Budget struct {
	mu              sync.Mutex
	workers, active int
	domains         map[string]*domainState
	changed         chan struct{}
}

func NewBudget(workers int) *Budget {
	if workers < 1 {
		panic("worker limit must be positive")
	}
	return &Budget{workers: workers, domains: make(map[string]*domainState), changed: make(chan struct{})}
}
func (b *Budget) signal() { close(b.changed); b.changed = make(chan struct{}) }
func (b *Budget) Acquire(ctx context.Context, domain string, interval time.Duration, concurrency int) (func(), error) {
	if domain == "" || len(domain) > 253 || domain != strings.ToLower(domain) || domain != strings.TrimSpace(domain) || interval <= 0 || concurrency < 1 {
		return nil, errors.New("invalid domain budget")
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		b.mu.Lock()
		state := b.domains[domain]
		if state == nil {
			state = &domainState{}
			b.domains[domain] = state
		}
		now := time.Now()
		ready := state.next
		if state.cooldown.After(ready) {
			ready = state.cooldown
		}
		if b.active < b.workers && state.active < concurrency && !now.Before(ready) {
			b.active++
			state.active++
			state.next = now.Add(interval)
			var once sync.Once
			b.mu.Unlock()
			return func() { once.Do(func() { b.mu.Lock(); b.active--; state.active--; b.signal(); b.mu.Unlock() }) }, nil
		}
		changed := b.changed
		wait := time.Until(ready)
		b.mu.Unlock()
		if wait <= 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-changed:
			}
			continue
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return nil, ctx.Err()
		case <-changed:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		case <-timer.C:
		}
	}
}

// Cooldown only extends a domain's current pause.
func (b *Budget) Cooldown(domain string, until time.Time) {
	if domain == "" || len(domain) > 253 || domain != strings.ToLower(domain) || domain != strings.TrimSpace(domain) {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	state := b.domains[domain]
	if state == nil {
		state = &domainState{}
		b.domains[domain] = state
	}
	if until.After(state.cooldown) {
		state.cooldown = until
		b.signal()
	}
}

// SourceBudget binds the common limiter to a reviewed source policy.
type SourceBudget struct {
	Budget      *Budget
	Domain      string
	Interval    time.Duration
	Concurrency int
}

func (s SourceBudget) Acquire(ctx context.Context) (func(), error) {
	if s.Budget == nil {
		return nil, errors.New("domain budget unavailable")
	}
	return s.Budget.Acquire(ctx, s.Domain, s.Interval, s.Concurrency)
}
