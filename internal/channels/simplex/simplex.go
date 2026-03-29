// Package simplex implements the SimpleX Chat channel for GoClaw.
// It connects to a running SimpleX CLI instance via its WebSocket API
// and bridges messages to/from the GoClaw message bus.
package simplex

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"

	"github.com/nextlevelbuilder/goclaw/internal/bus"
	"github.com/nextlevelbuilder/goclaw/internal/channels"
	"github.com/nextlevelbuilder/goclaw/internal/config"
)

// Channel connects to a SimpleX CLI instance via WebSocket.
type Channel struct {
	*channels.BaseChannel
	conn          *websocket.Conn
	config        config.SimpleXConfig
	mu            sync.Mutex
	connected     bool
	ctx           context.Context
	cancel        context.CancelFunc
	allowedGroups map[string]bool // set of allowed group IDs (empty = all allowed)
	chunkLimit    int             // max chars per outbound message
}

// New creates a new SimpleX channel from config.
func New(cfg config.SimpleXConfig, msgBus *bus.MessageBus) (*Channel, error) {
	if cfg.WebSocketURL == "" {
		return nil, fmt.Errorf("simplex websocket_url is required")
	}

	base := channels.NewBaseChannel(channels.TypeSimpleX, msgBus, cfg.AllowFrom)
	base.ValidatePolicy("disabled", cfg.GroupPolicy) // DMs disabled in V1

	allowedGroups := make(map[string]bool)
	for _, g := range cfg.AllowedGroups {
		allowedGroups[g] = true
	}

	chunkLimit := cfg.TextChunkLimit
	if chunkLimit <= 0 {
		chunkLimit = 4000
	}

	return &Channel{
		BaseChannel:   base,
		config:        cfg,
		allowedGroups: allowedGroups,
		chunkLimit:    chunkLimit,
	}, nil
}

// Start connects to the SimpleX CLI WebSocket and begins listening.
func (c *Channel) Start(ctx context.Context) error {
	slog.Info("starting simplex channel", "websocket_url", c.config.WebSocketURL)

	c.ctx, c.cancel = context.WithCancel(ctx)

	if err := c.connect(); err != nil {
		slog.Warn("initial simplex connection failed, will retry", "error", err)
	}

	go c.listenLoop()

	c.SetRunning(true)
	return nil
}

// BlockReplyEnabled returns the per-channel block_reply override (nil = inherit gateway default).
func (c *Channel) BlockReplyEnabled() *bool { return c.config.BlockReply }

// Stop gracefully shuts down the SimpleX channel.
func (c *Channel) Stop(_ context.Context) error {
	slog.Info("stopping simplex channel")

	if c.cancel != nil {
		c.cancel()
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}
	c.connected = false
	c.SetRunning(false)

	return nil
}

// Send delivers an outbound message to a SimpleX group.
func (c *Channel) Send(_ context.Context, msg bus.OutboundMessage) error {
	if msg.Content == "" {
		return nil
	}

	chunks := c.splitMessage(msg.Content)
	for _, chunk := range chunks {
		if err := c.sendText(msg.ChatID, chunk); err != nil {
			return err
		}
	}
	return nil
}

// sendText sends a single text message to a SimpleX group via the CLI protocol.
func (c *Channel) sendText(chatID, text string) error {
	composed := []map[string]any{
		{
			"msgContent": map[string]string{
				"type": "text",
				"text": text,
			},
		},
	}
	jsonBody, err := json.Marshal(composed)
	if err != nil {
		return fmt.Errorf("marshal simplex message: %w", err)
	}

	cmd := fmt.Sprintf("/_send #%s json %s", chatID, string(jsonBody))
	_, err = c.sendCommand(cmd)
	return err
}

// sendCommand sends a command to the SimpleX CLI and returns the raw response.
// It uses correlation IDs to match request/response pairs.
func (c *Channel) sendCommand(cmd string) (json.RawMessage, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn == nil {
		return nil, fmt.Errorf("simplex not connected")
	}

	corrID := uuid.New().String()
	payload := map[string]string{
		"corrId": corrID,
		"cmd":    cmd,
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal simplex command: %w", err)
	}

	if err := c.conn.WriteMessage(websocket.TextMessage, data); err != nil {
		return nil, fmt.Errorf("send simplex command: %w", err)
	}

	// Fire-and-forget for outbound sends. The response will arrive on the
	// read loop but we don't correlate it — outbound "newChatItems" with
	// chatDir.type "groupSnd" are simply ignored by handleEvent().
	return nil, nil
}

// splitMessage splits text into chunks respecting the SimpleX text limit.
func (c *Channel) splitMessage(text string) []string {
	if len(text) <= c.chunkLimit {
		return []string{text}
	}

	var chunks []string
	for len(text) > 0 {
		end := c.chunkLimit
		if end > len(text) {
			end = len(text)
		}
		// Try to split at a newline boundary.
		if end < len(text) {
			if idx := strings.LastIndex(text[:end], "\n"); idx > 0 {
				end = idx + 1
			}
		}
		chunks = append(chunks, text[:end])
		text = text[end:]
	}
	return chunks
}

// connect establishes the WebSocket connection to SimpleX CLI.
func (c *Channel) connect() error {
	dialer := websocket.DefaultDialer
	dialer.HandshakeTimeout = 10 * time.Second

	conn, _, err := dialer.Dial(c.config.WebSocketURL, nil)
	if err != nil {
		return fmt.Errorf("dial simplex %s: %w", c.config.WebSocketURL, err)
	}

	c.mu.Lock()
	c.conn = conn
	c.connected = true
	c.mu.Unlock()

	slog.Info("simplex websocket connected", "url", c.config.WebSocketURL)
	return nil
}

// listenLoop reads messages from SimpleX CLI with automatic reconnection.
func (c *Channel) listenLoop() {
	backoff := time.Second

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		c.mu.Lock()
		conn := c.conn
		c.mu.Unlock()

		if conn == nil {
			slog.Info("attempting simplex reconnect", "backoff", backoff)

			select {
			case <-c.ctx.Done():
				return
			case <-time.After(backoff):
			}

			if err := c.connect(); err != nil {
				slog.Warn("simplex reconnect failed", "error", err)
				backoff = min(backoff*2, 30*time.Second)
				continue
			}

			backoff = time.Second
			continue
		}

		_, message, err := conn.ReadMessage()
		if err != nil {
			slog.Warn("simplex read error, will reconnect", "error", err)

			c.mu.Lock()
			if c.conn != nil {
				_ = c.conn.Close()
				c.conn = nil
			}
			c.connected = false
			c.mu.Unlock()

			continue
		}

		c.handleRawMessage(message)
	}
}

// --- SimpleX CLI WebSocket protocol types ---

// wsMessage is the top-level WebSocket frame from SimpleX CLI.
type wsMessage struct {
	CorrID string          `json:"corrId,omitempty"`
	Resp   json.RawMessage `json:"resp"`
}

// wsResp is the parsed response envelope.
type wsResp struct {
	Type      string          `json:"type"`
	ChatItems json.RawMessage `json:"chatItems,omitempty"`
}

// simplexChatItem represents a single chat item from a newChatItems event.
type simplexChatItem struct {
	ChatInfo chatInfo `json:"chatInfo"`
	ChatItem chatItem `json:"chatItem"`
}

type chatInfo struct {
	Type      string     `json:"type"` // "direct" or "group"
	Contact   *contact   `json:"contact,omitempty"`
	GroupInfo *groupInfo `json:"groupInfo,omitempty"`
}

type contact struct {
	ContactID        json.Number `json:"contactId"`
	LocalDisplayName string      `json:"localDisplayName"`
}

type groupInfo struct {
	GroupID          json.Number `json:"groupId"`
	LocalDisplayName string      `json:"localDisplayName"`
}

type chatItem struct {
	ChatDir chatDir         `json:"chatDir"`
	Meta    chatItemMeta    `json:"meta"`
	Content chatItemContent `json:"content"`
	File    *chatItemFile   `json:"file,omitempty"`
}

type chatDir struct {
	Type        string       `json:"type"` // "directRcv", "groupRcv", "directSnd", "groupSnd"
	GroupMember *groupMember `json:"groupMember,omitempty"`
}

type groupMember struct {
	MemberID         string      `json:"memberId"`
	GroupMemberID    json.Number `json:"groupMemberId"`
	ContactID        json.Number `json:"contactId"`
	LocalDisplayName string      `json:"localDisplayName"`
}

type chatItemMeta struct {
	ItemID json.Number `json:"itemId"`
	ItemTs string      `json:"itemTs"`
}

type chatItemContent struct {
	Type       string      `json:"type"` // "rcvMsgContent", "sndMsgContent", etc.
	MsgContent *msgContent `json:"msgContent,omitempty"`
}

type msgContent struct {
	Type string `json:"type"` // "text", "voice", "image", "video", "file", etc.
	Text string `json:"text"`
}

type chatItemFile struct {
	FileID     json.Number     `json:"fileId"`
	FileName   string          `json:"fileName"`
	FileSize   int64           `json:"fileSize"`
	FileSource *chatFileSource `json:"fileSource,omitempty"`
}

type chatFileSource struct {
	FilePath string `json:"filePath"`
}

// handleRawMessage parses the top-level WebSocket frame and dispatches events.
func (c *Channel) handleRawMessage(raw []byte) {
	var frame wsMessage
	if err := json.Unmarshal(raw, &frame); err != nil {
		slog.Debug("simplex: invalid JSON frame", "error", err)
		return
	}

	// If this has a corrId, it's a response to a command we sent.
	// We use fire-and-forget for sends, so just ignore correlated responses.
	if frame.CorrID != "" {
		return
	}

	var resp wsResp
	if err := json.Unmarshal(frame.Resp, &resp); err != nil {
		slog.Debug("simplex: cannot parse resp", "error", err)
		return
	}

	switch resp.Type {
	case "newChatItems":
		c.handleNewChatItems(resp.ChatItems)
	default:
		// Ignore other event types (chatItemStatusUpdated, contactConnected, etc.)
	}
}

// handleNewChatItems processes inbound chat items from a newChatItems event.
func (c *Channel) handleNewChatItems(raw json.RawMessage) {
	var items []simplexChatItem
	if err := json.Unmarshal(raw, &items); err != nil {
		slog.Warn("simplex: cannot parse chatItems", "error", err)
		return
	}

	for _, item := range items {
		c.handleChatItem(item)
	}
}

// handleChatItem processes a single inbound chat item.
func (c *Channel) handleChatItem(item simplexChatItem) {
	// Only process received messages (not our own outbound).
	dirType := item.ChatItem.ChatDir.Type
	if dirType != "groupRcv" && dirType != "directRcv" {
		return
	}

	// Only process actual message content.
	if item.ChatItem.Content.Type != "rcvMsgContent" {
		return
	}

	// V1: Only group messages.
	if item.ChatInfo.Type != "group" {
		slog.Debug("simplex: ignoring DM (V1 groups only)", "type", item.ChatInfo.Type)
		return
	}

	if item.ChatInfo.GroupInfo == nil {
		slog.Debug("simplex: group message without groupInfo")
		return
	}

	groupID := item.ChatInfo.GroupInfo.GroupID.String()

	// Group policy check.
	if !c.checkGroupPolicy(groupID) {
		slog.Debug("simplex: group rejected by policy", "group_id", groupID)
		return
	}

	// Extract sender info.
	senderID, senderName := c.extractSender(item)
	if senderID == "" {
		slog.Debug("simplex: cannot determine sender")
		return
	}

	// Extract message text.
	text := ""
	if item.ChatItem.Content.MsgContent != nil {
		text = item.ChatItem.Content.MsgContent.Text
	}
	if text == "" {
		// Skip empty text messages (voice/media without text handled later).
		return
	}

	metadata := map[string]string{
		"user_name":  senderName,
		"message_id": item.ChatItem.Meta.ItemID.String(),
		"group_name": item.ChatInfo.GroupInfo.LocalDisplayName,
	}

	slog.Debug("simplex message received",
		"sender_id", senderID,
		"group_id", groupID,
		"preview", channels.Truncate(text, 50),
	)

	c.HandleMessage(senderID, groupID, text, nil, metadata, "group")
}

// extractSender resolves the sender ID and display name from a chat item.
// Priority: contactId > memberId > groupMemberId.
func (c *Channel) extractSender(item simplexChatItem) (id, name string) {
	gm := item.ChatItem.ChatDir.GroupMember
	if gm == nil {
		return "", ""
	}

	name = gm.LocalDisplayName

	// Try contactId first (most stable identifier).
	if cid := gm.ContactID.String(); cid != "" && cid != "0" {
		return cid, name
	}
	// Fallback to memberId.
	if gm.MemberID != "" {
		return gm.MemberID, name
	}
	// Last resort: groupMemberId.
	if gmid := gm.GroupMemberID.String(); gmid != "" && gmid != "0" {
		return gmid, name
	}

	return "", name
}

// checkGroupPolicy evaluates whether a group is allowed.
func (c *Channel) checkGroupPolicy(groupID string) bool {
	policy := c.config.GroupPolicy
	if policy == "" {
		policy = "open"
	}

	switch policy {
	case "disabled":
		return false
	case "allowlist":
		return c.allowedGroups[groupID]
	default: // "open"
		return true
	}
}

// IsGroupAllowed checks if a specific group ID is in the allowed list.
// Used for external policy checks.
func (c *Channel) IsGroupAllowed(groupID string) bool {
	if len(c.allowedGroups) == 0 {
		return true
	}
	_, ok := c.allowedGroups[groupID]
	// Also check with string conversion in case of numeric mismatch.
	if !ok {
		if n, err := strconv.Atoi(groupID); err == nil {
			_, ok = c.allowedGroups[strconv.Itoa(n)]
		}
	}
	return ok
}
