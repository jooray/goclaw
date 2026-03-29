package simplex

import (
	"encoding/json"
	"fmt"

	"github.com/nextlevelbuilder/goclaw/internal/bus"
	"github.com/nextlevelbuilder/goclaw/internal/channels"
	"github.com/nextlevelbuilder/goclaw/internal/config"
	"github.com/nextlevelbuilder/goclaw/internal/store"
)

// simplexCreds maps the credentials JSON from the channel_instances table.
type simplexCreds struct {
	WebSocketURL string `json:"websocket_url"`
}

// simplexInstanceConfig maps the non-secret config JSONB from the channel_instances table.
type simplexInstanceConfig struct {
	GroupPolicy    string   `json:"group_policy,omitempty"`
	AllowedGroups  []string `json:"allowed_groups,omitempty"`
	AllowFrom      []string `json:"allow_from,omitempty"`
	TextChunkLimit int      `json:"text_chunk_limit,omitempty"`
	BlockReply     *bool    `json:"block_reply,omitempty"`
}

// Factory creates a SimpleX channel from DB instance data.
func Factory(name string, creds json.RawMessage, cfg json.RawMessage,
	msgBus *bus.MessageBus, _ store.PairingStore) (channels.Channel, error) {

	var c simplexCreds
	if len(creds) > 0 {
		if err := json.Unmarshal(creds, &c); err != nil {
			return nil, fmt.Errorf("decode simplex credentials: %w", err)
		}
	}
	if c.WebSocketURL == "" {
		return nil, fmt.Errorf("simplex websocket_url is required")
	}

	var ic simplexInstanceConfig
	if len(cfg) > 0 {
		if err := json.Unmarshal(cfg, &ic); err != nil {
			return nil, fmt.Errorf("decode simplex config: %w", err)
		}
	}

	sxCfg := config.SimpleXConfig{
		Enabled:        true,
		WebSocketURL:   c.WebSocketURL,
		AllowFrom:      ic.AllowFrom,
		GroupPolicy:    ic.GroupPolicy,
		AllowedGroups:  ic.AllowedGroups,
		TextChunkLimit: ic.TextChunkLimit,
		BlockReply:     ic.BlockReply,
	}

	// DB instances default to "allowlist" for groups (secure by default).
	if sxCfg.GroupPolicy == "" {
		sxCfg.GroupPolicy = "allowlist"
	}

	ch, err := New(sxCfg, msgBus)
	if err != nil {
		return nil, err
	}

	ch.SetName(name)
	return ch, nil
}
