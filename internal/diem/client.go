package diem

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// VeniceBalanceClient fetches DIEM balance from the Venice billing API.
// It uses the admin API key (separate from the inference key).
type VeniceBalanceClient struct {
	adminKey string
	apiBase  string
	client   *http.Client
}

// veniceBalanceResponse matches the Venice /api/v1/billing/balance JSON shape.
type veniceBalanceResponse struct {
	Balances            map[string]float64 `json:"balances"`
	DiemEpochAllocation float64            `json:"diemEpochAllocation"`
	CanConsume          bool               `json:"canConsume"`
	ConsumptionCurrency string             `json:"consumptionCurrency"`
}

// NewVeniceBalanceClient creates a balance client.
// apiBase defaults to "https://api.venice.ai/api/v1" if empty.
func NewVeniceBalanceClient(adminKey, apiBase string) *VeniceBalanceClient {
	if apiBase == "" {
		apiBase = "https://api.venice.ai/api/v1"
	}
	return &VeniceBalanceClient{
		adminKey: adminKey,
		apiBase:  apiBase,
		client: &http.Client{
			Timeout: 20 * time.Second,
		},
	}
}

// Fetch implements BalanceProvider.
func (c *VeniceBalanceClient) Fetch(ctx context.Context) (BalanceState, error) {
	url := c.apiBase + "/billing/balance"

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return BalanceState{}, fmt.Errorf("diem: build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.adminKey)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "goclaw-diem/1.0")

	resp, err := c.client.Do(req)
	if err != nil {
		return BalanceState{}, fmt.Errorf("diem: fetch balance: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	if err != nil {
		return BalanceState{}, fmt.Errorf("diem: read body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return BalanceState{}, fmt.Errorf("diem: billing API returned %d: %s", resp.StatusCode, string(body))
	}

	var payload veniceBalanceResponse
	if err := json.Unmarshal(body, &payload); err != nil {
		return BalanceState{}, fmt.Errorf("diem: parse response: %w", err)
	}

	remaining, ok := payload.Balances["diem"]
	if !ok {
		return BalanceState{}, fmt.Errorf("diem: response missing diem balance")
	}
	total := payload.DiemEpochAllocation
	if total <= 0 {
		return BalanceState{}, fmt.Errorf("diem: invalid epoch allocation: %f", total)
	}

	used := total - remaining
	pct := (used / total) * 100
	if pct < 0 {
		pct = 0
	}
	if pct > 100 {
		pct = 100
	}

	return BalanceState{
		CheckedAt:   time.Now().UTC(),
		PercentUsed: pct,
		Remaining:   remaining,
		Total:       total,
		CanConsume:  payload.CanConsume,
		Source:      "venice-admin-api",
	}, nil
}
