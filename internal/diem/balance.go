// Package diem implements the Venice DIEM budget router.
//
// The router wraps a Venice LLM provider and selects the effective model
// based on cached daily DIEM spend percentage.  It refreshes balance only
// when a Venice-backed inference is about to run and the cache is stale
// (older than the configured TTL, default 10 minutes).
//
// See goclaw-specification.md section 9 for the full specification.
package diem

import (
	"context"
	"sync"
	"time"
)

// BalanceState holds a point-in-time DIEM balance snapshot.
type BalanceState struct {
	CheckedAt   time.Time
	PercentUsed float64 // 0-100
	Remaining   float64 // raw remaining DIEM
	Total       float64 // epoch allocation
	CanConsume  bool
	Source      string // e.g. "venice-admin-api"
}

// BalanceProvider fetches the current DIEM balance from an external source.
type BalanceProvider interface {
	Fetch(ctx context.Context) (BalanceState, error)
}

// BalanceCache wraps a BalanceProvider with a time-based cache.
// Only one in-flight fetch runs at a time (coalesced).
type BalanceCache struct {
	provider BalanceProvider
	ttl      time.Duration

	mu    sync.Mutex
	state *BalanceState
}

// NewBalanceCache creates a cache with the given TTL.
// If ttl <= 0, defaults to 10 minutes.
func NewBalanceCache(provider BalanceProvider, ttl time.Duration) *BalanceCache {
	if ttl <= 0 {
		ttl = 10 * time.Minute
	}
	return &BalanceCache{
		provider: provider,
		ttl:      ttl,
	}
}

// Get returns the cached balance, refreshing if stale or absent.
// The caller blocks until the fetch completes.
func (c *BalanceCache) Get(ctx context.Context) (BalanceState, error) {
	c.mu.Lock()
	if c.state != nil && time.Since(c.state.CheckedAt) <= c.ttl {
		s := *c.state
		c.mu.Unlock()
		return s, nil
	}
	c.mu.Unlock()

	// Fetch outside lock to avoid blocking other goroutines on network I/O.
	s, err := c.provider.Fetch(ctx)
	if err != nil {
		// Return stale data if available rather than failing hard.
		c.mu.Lock()
		if c.state != nil {
			stale := *c.state
			c.mu.Unlock()
			return stale, nil
		}
		c.mu.Unlock()
		return BalanceState{}, err
	}

	c.mu.Lock()
	c.state = &s
	c.mu.Unlock()
	return s, nil
}

// Invalidate forces the next Get to refresh.
func (c *BalanceCache) Invalidate() {
	c.mu.Lock()
	c.state = nil
	c.mu.Unlock()
}

// Peek returns the cached state without refreshing.  Returns nil if no data.
func (c *BalanceCache) Peek() *BalanceState {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state == nil {
		return nil
	}
	cp := *c.state
	return &cp
}
