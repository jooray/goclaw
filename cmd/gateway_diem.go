package cmd

import (
	"log/slog"
	"time"

	"github.com/nextlevelbuilder/goclaw/internal/config"
	"github.com/nextlevelbuilder/goclaw/internal/diem"
	"github.com/nextlevelbuilder/goclaw/internal/providers"
)

// setupDiemRouter wraps the Venice provider in the DIEM budget router.
// It replaces the "venice" provider in the registry with a DiemRouter wrapper
// that transparently selects models based on cached DIEM spend percentage.
//
// No-op if DIEM is not enabled or Venice admin key is missing.
func setupDiemRouter(registry *providers.Registry, cfg *config.Config) {
	diemCfg := cfg.Diem
	if !diemCfg.Enabled {
		slog.Debug("diem: routing disabled")
		return
	}
	if diemCfg.AdminKey == "" {
		slog.Warn("diem: enabled but GOCLAW_VENICE_ADMIN_KEY not set, skipping")
		return
	}

	// Resolve the Venice provider from registry.
	providerName := diemCfg.ProviderName
	if providerName == "" {
		providerName = "venice"
	}
	veniceProvider, err := registry.GetForTenant(providers.MasterTenantID, providerName)
	if err != nil {
		slog.Warn("diem: venice provider not found in registry, skipping",
			"name", providerName, "error", err)
		return
	}

	// Lower Venice provider's retry count to protect DIEM budget.
	// Venice charges for each 503 retry attempt, so we cap at maxRetries.
	maxRetries := diemCfg.MaxRetries
	if maxRetries <= 0 {
		maxRetries = 2
	}
	if oai, ok := veniceProvider.(*providers.OpenAIProvider); ok {
		retryCfg := providers.DefaultRetryConfig()
		retryCfg.Attempts = maxRetries
		oai.WithRetryConfig(retryCfg)
		slog.Info("diem: venice retry config lowered", "attempts", maxRetries)
	}

	// Resolve the fallback provider (optional).
	var fallbackProvider providers.Provider
	fallbackName := diemCfg.FallbackName
	if fallbackName == "" {
		fallbackName = "ollama-cloud"
	}
	fallbackProvider, err = registry.GetForTenant(providers.MasterTenantID, fallbackName)
	if err != nil {
		slog.Info("diem: fallback provider not found, no fallback configured",
			"name", fallbackName)
	}

	fallbackModel := diemCfg.FallbackModel
	if fallbackModel == "" {
		fallbackModel = "qwen3.5:397b-cloud"
	}

	// Build the balance client.
	apiBase := diemCfg.APIBase
	if apiBase == "" {
		apiBase = "https://api.venice.ai/api/v1"
	}
	balanceClient := diem.NewVeniceBalanceClient(diemCfg.AdminKey, apiBase)

	// Build the balance cache.
	ttl := 10 * time.Minute
	if diemCfg.CacheTTLSecs > 0 {
		ttl = time.Duration(diemCfg.CacheTTLSecs) * time.Second
	}
	balanceCache := diem.NewBalanceCache(balanceClient, ttl)

	// Build the tier resolver.
	var tiers []diem.Tier
	if len(diemCfg.Tiers) > 0 {
		for _, t := range diemCfg.Tiers {
			tiers = append(tiers, diem.Tier{
				MaxPercent: t.MaxPercent,
				Model:      t.Model,
			})
		}
	} else {
		tiers = diem.DefaultTiers()
	}
	tierResolver := diem.NewTierResolver(tiers, "")

	// Build the router.
	router := diem.NewRouter(diem.RouterConfig{
		VeniceProvider:   veniceProvider,
		FallbackProvider: fallbackProvider,
		FallbackModel:    fallbackModel,
		Balance:          balanceCache,
		Tiers:            tierResolver,
		Pin:              diem.DefaultPinPolicy(),
		MaxVeniceRetries: maxRetries,
	})

	// Replace the Venice provider in the registry with the DIEM router.
	// The router implements providers.Provider, so it's transparent to the rest of GoClaw.
	registry.Register(router)

	slog.Info("diem: router enabled",
		"venice", providerName,
		"fallback", fallbackName,
		"fallback_model", fallbackModel,
		"cache_ttl", ttl,
		"max_retries", maxRetries,
		"tiers", len(tiers),
	)
}
