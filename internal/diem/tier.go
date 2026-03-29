package diem

import (
	"log/slog"
	"sync"
)

// Tier maps a DIEM spend range to a model name.
type Tier struct {
	MaxPercent float64 // upper bound (inclusive) of the spend percentage
	Model      string  // model to use in this tier
}

// TierResolver selects a model based on DIEM spend percentage.
// It also detects DIEM epoch resets (spend drops from >30% to <10%).
type TierResolver struct {
	tiers         []Tier // ordered by MaxPercent ascending
	fallbackModel string // model when no tier matches (>100%)

	mu       sync.Mutex
	lastPct  float64 // previous spend percentage (for reset detection)
	hasState bool    // true after first call
}

// DefaultTiers returns the standard Victoria tier table.
func DefaultTiers() []Tier {
	return []Tier{
		{MaxPercent: 34, Model: "claude-sonnet-4-6"},
		{MaxPercent: 64, Model: "grok-4-20-beta"},
		{MaxPercent: 89, Model: "gemini-3-flash-preview"},
		{MaxPercent: 100, Model: "grok-41-fast"},
	}
}

// NewTierResolver creates a resolver with the given tier table.
// The tiers must be sorted by MaxPercent ascending.
// fallbackModel is used when spend exceeds all tiers (shouldn't happen normally).
func NewTierResolver(tiers []Tier, fallbackModel string) *TierResolver {
	if len(tiers) == 0 {
		tiers = DefaultTiers()
	}
	if fallbackModel == "" && len(tiers) > 0 {
		fallbackModel = tiers[len(tiers)-1].Model
	}
	return &TierResolver{
		tiers:         tiers,
		fallbackModel: fallbackModel,
	}
}

// Resolve returns the model name for the given balance state.
// It handles reset detection: if prior spend was >30% and current is <10%,
// it resets to the top tier (epoch boundary crossed).
func (r *TierResolver) Resolve(state BalanceState) string {
	pct := state.PercentUsed

	r.mu.Lock()
	defer r.mu.Unlock()

	// Reset detection: DIEM epoch rolled over (00:00 UTC).
	if r.hasState && r.lastPct > 30 && pct < 10 {
		slog.Info("diem: epoch reset detected",
			"prev_pct", r.lastPct,
			"new_pct", pct,
		)
	}

	r.lastPct = pct
	r.hasState = true

	// Find the matching tier.
	for _, t := range r.tiers {
		if pct <= t.MaxPercent {
			return t.Model
		}
	}

	return r.fallbackModel
}

// PinPolicy defines per-run model pinning rules.
type PinPolicy struct {
	// HeartbeatModel is always used for heartbeat runs (empty = no pin).
	HeartbeatModel string
}

// DefaultPinPolicy returns the standard pinning config.
func DefaultPinPolicy() PinPolicy {
	return PinPolicy{
		HeartbeatModel: "grok-41-fast",
	}
}
