package diem

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/nextlevelbuilder/goclaw/internal/providers"
)

// RouterConfig configures the DIEM router.
type RouterConfig struct {
	// VeniceProvider is the underlying Venice openai_compat provider.
	VeniceProvider providers.Provider

	// FallbackProvider is used when Venice is exhausted (e.g. ollama-cloud).
	// May be nil if no fallback is configured.
	FallbackProvider providers.Provider
	FallbackModel    string // model to use with fallback provider

	// Balance is the cached DIEM balance source.
	Balance *BalanceCache

	// Tiers resolves spend percentage to model name.
	Tiers *TierResolver

	// Pin overrides model selection for specific run types.
	Pin PinPolicy

	// MaxVeniceRetries caps retry attempts for Venice 503 errors.
	// Default: 2 (1 original + 1 retry, less than the global default of 3).
	MaxVeniceRetries int
}

// Router implements providers.Provider by wrapping Venice with DIEM-aware
// model selection and fallback routing.
//
// It follows the same wrapper pattern as ChatGPTOAuthRouter:
//   - Implements Provider interface (Chat, ChatStream, DefaultModel, Name)
//   - Delegates to underlying providers after resolving the route
//   - Does NOT modify the provider registry; it IS the registered provider
type Router struct {
	venice   providers.Provider
	fallback providers.Provider
	fbModel  string

	balance *BalanceCache
	tiers   *TierResolver
	pin     PinPolicy

	maxRetries int
}

// NewRouter creates a DIEM-aware provider wrapper.
func NewRouter(cfg RouterConfig) *Router {
	maxRetries := cfg.MaxVeniceRetries
	if maxRetries <= 0 {
		maxRetries = 2
	}
	return &Router{
		venice:     cfg.VeniceProvider,
		fallback:   cfg.FallbackProvider,
		fbModel:    cfg.FallbackModel,
		balance:    cfg.Balance,
		tiers:      cfg.Tiers,
		pin:        cfg.Pin,
		maxRetries: maxRetries,
	}
}

// Name returns "venice" so the router is transparent to the rest of GoClaw.
func (r *Router) Name() string {
	return r.venice.Name()
}

// DefaultModel resolves the current DIEM-appropriate model.
func (r *Router) DefaultModel() string {
	state, err := r.balance.Get(context.Background())
	if err != nil {
		slog.Warn("diem: balance fetch failed for DefaultModel, using top tier", "error", err)
		if len(r.tiers.tiers) > 0 {
			return r.tiers.tiers[0].Model
		}
		return r.venice.DefaultModel()
	}
	return r.tiers.Resolve(state)
}

// SupportsThinking delegates to the underlying Venice provider.
func (r *Router) SupportsThinking() bool {
	if tc, ok := r.venice.(providers.ThinkingCapable); ok {
		return tc.SupportsThinking()
	}
	return false
}

// resolvedRoute holds the routing decision for a single call.
type resolvedRoute struct {
	provider providers.Provider
	model    string
	reason   string // for logging
}

// resolveRoute determines which provider and model to use for this call.
// It checks the request model for heartbeat pins, then consults DIEM balance.
func (r *Router) resolveRoute(ctx context.Context, reqModel string) resolvedRoute {
	// If the request already specifies a model (e.g. heartbeat override),
	// check if it's a heartbeat pin.
	if reqModel != "" && r.pin.HeartbeatModel != "" {
		// Strip "venice/" prefix for comparison.
		clean := strings.TrimPrefix(reqModel, "venice/")
		if clean == r.pin.HeartbeatModel {
			return resolvedRoute{
				provider: r.venice,
				model:    reqModel,
				reason:   "heartbeat-pin",
			}
		}
	}

	// Fetch DIEM balance (from cache or refresh if stale).
	state, err := r.balance.Get(ctx)
	if err != nil {
		slog.Warn("diem: balance unavailable, using top tier", "error", err)
		model := r.tiers.tiers[0].Model
		return resolvedRoute{
			provider: r.venice,
			model:    model,
			reason:   "balance-error-fallback",
		}
	}

	// Check if Venice is completely exhausted.
	if !state.CanConsume && r.fallback != nil {
		slog.Info("diem: Venice cannot consume, routing to fallback",
			"pct", state.PercentUsed,
			"fallback", r.fallback.Name(),
			"model", r.fbModel,
		)
		return resolvedRoute{
			provider: r.fallback,
			model:    r.fbModel,
			reason:   "diem-exhausted",
		}
	}

	// Resolve tier based on spend.
	model := r.tiers.Resolve(state)

	slog.Debug("diem: route resolved",
		"pct", fmt.Sprintf("%.1f", state.PercentUsed),
		"model", model,
		"remaining", fmt.Sprintf("%.0f", state.Remaining),
	)

	return resolvedRoute{
		provider: r.venice,
		model:    model,
		reason:   "diem-tier",
	}
}

// Chat implements providers.Provider.
func (r *Router) Chat(ctx context.Context, req providers.ChatRequest) (*providers.ChatResponse, error) {
	route := r.resolveRoute(ctx, req.Model)
	req.Model = route.model

	slog.Info("diem: Chat",
		"provider", route.provider.Name(),
		"model", route.model,
		"reason", route.reason,
	)

	resp, err := r.callWithBudget(ctx, route, func(p providers.Provider) (*providers.ChatResponse, error) {
		return p.Chat(ctx, req)
	})
	return resp, err
}

// ChatStream implements providers.Provider.
func (r *Router) ChatStream(ctx context.Context, req providers.ChatRequest, onChunk func(providers.StreamChunk)) (*providers.ChatResponse, error) {
	route := r.resolveRoute(ctx, req.Model)
	req.Model = route.model

	slog.Info("diem: ChatStream",
		"provider", route.provider.Name(),
		"model", route.model,
		"reason", route.reason,
	)

	resp, err := r.callWithBudget(ctx, route, func(p providers.Provider) (*providers.ChatResponse, error) {
		return p.ChatStream(ctx, req, onChunk)
	})
	return resp, err
}

// callWithBudget wraps a provider call with DIEM-specific retry budget:
//   - 402: abort immediately (budget exhausted), try fallback
//   - 503: retry up to maxRetries (less than global default)
//   - other retryable: delegate to the underlying provider's built-in retry
func (r *Router) callWithBudget(
	ctx context.Context,
	route resolvedRoute,
	fn func(providers.Provider) (*providers.ChatResponse, error),
) (*providers.ChatResponse, error) {
	resp, err := fn(route.provider)
	if err == nil {
		return resp, nil
	}

	// Check for 402 (budget exhausted).
	var httpErr *providers.HTTPError
	if errors.As(err, &httpErr) && httpErr.Status == 402 {
		slog.Warn("diem: 402 budget exhausted",
			"provider", route.provider.Name(),
			"model", route.model,
		)
		// Invalidate cache so next call re-checks balance.
		r.balance.Invalidate()

		// Try fallback if available and not already on fallback.
		if r.fallback != nil && route.provider != r.fallback {
			slog.Info("diem: falling back after 402",
				"fallback", r.fallback.Name(),
				"model", r.fbModel,
			)
			fbResp, fbErr := fn(r.fallback)
			if fbErr == nil {
				return fbResp, nil
			}
			return nil, fmt.Errorf("diem: fallback also failed: %w (original: %v)", fbErr, err)
		}
		return nil, fmt.Errorf("diem: budget exhausted (402): %w", err)
	}

	// For 503 on Venice: the underlying provider's RetryDo already retried
	// up to its configured attempts.  We don't add extra retries here;
	// instead, we ensure Venice is configured with lower retry count
	// (see RouterConfig.MaxVeniceRetries, applied during wiring).
	//
	// If Venice failed with 503 and fallback is available, try fallback.
	if errors.As(err, &httpErr) && httpErr.Status == 503 {
		slog.Warn("diem: 503 from Venice after retries",
			"provider", route.provider.Name(),
			"model", route.model,
		)
		if r.fallback != nil && route.provider != r.fallback {
			slog.Info("diem: falling back after 503",
				"fallback", r.fallback.Name(),
				"model", r.fbModel,
			)
			fbResp, fbErr := fn(r.fallback)
			if fbErr == nil {
				return fbResp, nil
			}
			return nil, fmt.Errorf("diem: fallback also failed: %w (original: %v)", fbErr, err)
		}
	}

	return nil, err
}
