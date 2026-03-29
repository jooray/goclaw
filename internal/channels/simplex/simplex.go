// Package simplex implements the SimpleX Chat channel for GoClaw.
// It connects to a running SimpleX CLI instance via its WebSocket API
// and bridges messages to/from the GoClaw message bus.
package simplex

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"

	"github.com/nextlevelbuilder/goclaw/internal/bus"
	"github.com/nextlevelbuilder/goclaw/internal/channels"
	"github.com/nextlevelbuilder/goclaw/internal/channels/media"
	"github.com/nextlevelbuilder/goclaw/internal/config"
)

const (
	// pendingFileTimeout is how long to wait for a file download to complete.
	pendingFileTimeout = 90 * time.Second
)

// pendingFile tracks a file download that's in progress.
type pendingFile struct {
	item    simplexChatItem // the original chat item
	timer   *time.Timer     // timeout timer
	created time.Time
}

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

	// pendingFiles tracks file downloads keyed by fileId string.
	pendingMu    sync.Mutex
	pendingFiles map[string]*pendingFile
}

// New creates a new SimpleX channel from config.
func New(cfg config.SimpleXConfig, msgBus *bus.MessageBus) (*Channel, error) {
	if cfg.WebSocketURL == "" {
		return nil, fmt.Errorf("simplex websocket_url is required")
	}

	slog.Info("simplex: channel config loaded",
		"websocket_url", cfg.WebSocketURL,
		"stt_proxy_url", cfg.STTProxyURL,
		"stt_timeout", cfg.STTTimeoutSeconds,
		"group_policy", cfg.GroupPolicy,
	)

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
		pendingFiles:  make(map[string]*pendingFile),
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

	// Cancel all pending file timers.
	c.pendingMu.Lock()
	for k, pf := range c.pendingFiles {
		pf.timer.Stop()
		delete(c.pendingFiles, k)
	}
	c.pendingMu.Unlock()

	return nil
}

// Send delivers an outbound message to a SimpleX group.
// Handles voice messages when audio_as_voice metadata is set.
func (c *Channel) Send(_ context.Context, msg bus.OutboundMessage) error {
	// Check for voice/audio media with audio_as_voice flag.
	if msg.Metadata != nil && msg.Metadata["audio_as_voice"] == "true" && len(msg.Media) > 0 {
		for _, att := range msg.Media {
			if strings.HasPrefix(att.ContentType, "audio/") || strings.HasSuffix(att.URL, ".wav") ||
				strings.HasSuffix(att.URL, ".ogg") || strings.HasSuffix(att.URL, ".m4a") ||
				strings.HasSuffix(att.URL, ".mp3") {
				if err := c.sendVoice(msg.ChatID, att.URL, msg.Content); err != nil {
					slog.Warn("simplex: voice send failed, falling back to text", "error", err)
				} else {
					// Voice sent successfully; send any remaining text.
					if msg.Content != "" {
						return c.sendTextChunked(msg.ChatID, msg.Content)
					}
					return nil
				}
			}
		}
	}

	// Regular text send (or fallback from voice).
	if msg.Content == "" && len(msg.Media) == 0 {
		return nil
	}

	// Send any non-voice media as files.
	for _, att := range msg.Media {
		if err := c.sendFile(msg.ChatID, att.URL); err != nil {
			slog.Warn("simplex: file send failed", "path", att.URL, "error", err)
		}
	}

	return c.sendTextChunked(msg.ChatID, msg.Content)
}

// sendTextChunked sends text, splitting into chunks if needed.
func (c *Channel) sendTextChunked(chatID, text string) error {
	if text == "" {
		return nil
	}
	chunks := c.splitMessage(text)
	for _, chunk := range chunks {
		if err := c.sendText(chatID, chunk); err != nil {
			return err
		}
	}
	return nil
}

// sendVoice sends a voice note to a SimpleX group.
// If the file is WAV, it converts to m4a first (SimpleX expects m4a/ogg for voice bubbles).
func (c *Channel) sendVoice(chatID, audioPath, caption string) error {
	finalPath := audioPath

	// Convert WAV to m4a for SimpleX voice bubble compatibility.
	if strings.HasSuffix(strings.ToLower(audioPath), ".wav") {
		m4aPath := strings.TrimSuffix(audioPath, filepath.Ext(audioPath)) + ".m4a"
		cmd := exec.CommandContext(c.ctx, "ffmpeg", "-y", "-i", audioPath,
			"-c:a", "aac", "-b:a", "64k", "-movflags", "+faststart", m4aPath)
		if out, err := cmd.CombinedOutput(); err != nil {
			slog.Warn("simplex: ffmpeg WAV->m4a conversion failed",
				"error", err, "output", string(out))
			return fmt.Errorf("ffmpeg WAV->m4a: %w", err)
		}
		finalPath = m4aPath
		slog.Debug("simplex: converted WAV to m4a", "src", audioPath, "dst", m4aPath)
	}

	composed := []map[string]any{
		{
			"fileSource": map[string]string{
				"filePath": finalPath,
			},
			"msgContent": map[string]any{
				"type":     "voice",
				"text":     caption,
				"duration": 0,
			},
		},
	}
	jsonBody, err := json.Marshal(composed)
	if err != nil {
		return fmt.Errorf("marshal simplex voice message: %w", err)
	}

	cmd := fmt.Sprintf("/_send #%s json %s", chatID, string(jsonBody))
	_, err = c.sendCommand(cmd)
	if err != nil {
		return fmt.Errorf("send simplex voice: %w", err)
	}

	slog.Debug("simplex: voice note sent", "chat_id", chatID, "path", finalPath)
	return nil
}

// sendFile sends a file attachment to a SimpleX group.
func (c *Channel) sendFile(chatID, filePath string) error {
	composed := []map[string]any{
		{
			"fileSource": map[string]string{
				"filePath": filePath,
			},
			"msgContent": map[string]string{
				"type": "file",
				"text": "",
			},
		},
	}
	jsonBody, err := json.Marshal(composed)
	if err != nil {
		return fmt.Errorf("marshal simplex file message: %w", err)
	}

	cmd := fmt.Sprintf("/_send #%s json %s", chatID, string(jsonBody))
	_, err = c.sendCommand(cmd)
	return err
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
	// For rcvFileDescrReady / rcvFileComplete events
	ChatItem        json.RawMessage `json:"chatItem,omitempty"`
	RcvFileTransfer json.RawMessage `json:"rcvFileTransfer,omitempty"`
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
	Type     string `json:"type"` // "text", "voice", "image", "video", "file", etc.
	Text     string `json:"text"`
	Duration int    `json:"duration,omitempty"` // for voice messages
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

// rcvFileTransferInfo is used to extract fileId from rcvFileDescrReady events.
type rcvFileTransferInfo struct {
	FileID json.Number `json:"fileId"`
}

// handleRawMessage parses the top-level WebSocket frame and dispatches events.
func (c *Channel) handleRawMessage(raw []byte) {
	var frame wsMessage
	if err := json.Unmarshal(raw, &frame); err != nil {
		slog.Warn("simplex: invalid JSON frame", "error", err)
		return
	}

	// If this has a corrId, it's a response to a command we sent.
	// We use fire-and-forget for sends, so just ignore correlated responses.
	if frame.CorrID != "" {
		return
	}

	var resp wsResp
	if err := json.Unmarshal(frame.Resp, &resp); err != nil {
		slog.Warn("simplex: cannot parse resp", "error", err)
		return
	}

	slog.Debug("simplex: ws event", "type", resp.Type)

	switch resp.Type {
	case "newChatItems":
		c.handleNewChatItems(resp.ChatItems)
	case "rcvFileDescrReady":
		c.handleRcvFileDescrReady(resp.RcvFileTransfer)
	case "rcvFileComplete":
		c.handleRcvFileComplete(resp.ChatItem)
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

	// Determine content type.
	msgType := ""
	if item.ChatItem.Content.MsgContent != nil {
		msgType = item.ChatItem.Content.MsgContent.Type
	}

	hasFile := item.ChatItem.File != nil
	slog.Debug("simplex: chat item received",
		"msg_type", msgType,
		"has_file", hasFile,
		"dir_type", dirType,
		"sender", senderName,
	)

	// If this message has a file attachment, check if we need to wait for download.
	if hasFile && (msgType == "voice" || msgType == "audio" || msgType == "image" || msgType == "file" || msgType == "video") {
		fileID := item.ChatItem.File.FileID.String()

		// Request file download.
		if _, err := c.sendCommand(fmt.Sprintf("/freceive %s", fileID)); err != nil {
			slog.Warn("simplex: freceive failed", "file_id", fileID, "error", err)
			// Fall through to handle as text-only message.
		} else {
			slog.Debug("simplex: requested file download",
				"file_id", fileID,
				"msg_type", msgType,
				"file_name", item.ChatItem.File.FileName,
			)

			// Store as pending — will be finalized on rcvFileComplete.
			c.pendingMu.Lock()
			timer := time.AfterFunc(pendingFileTimeout, func() {
				c.handlePendingTimeout(fileID)
			})
			c.pendingFiles[fileID] = &pendingFile{
				item:    item,
				timer:   timer,
				created: time.Now(),
			}
			c.pendingMu.Unlock()
			return // Don't process yet — wait for file download.
		}
	}

	// Process as a text message (or text-with-no-file).
	text := ""
	if item.ChatItem.Content.MsgContent != nil {
		text = item.ChatItem.Content.MsgContent.Text
	}
	if text == "" {
		// Skip empty text messages with no file attachment.
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

// handleRcvFileDescrReady handles early file descriptor notifications.
// This is sent before newChatItems for some file types. We auto-accept the download.
func (c *Channel) handleRcvFileDescrReady(raw json.RawMessage) {
	if raw == nil {
		return
	}

	var info rcvFileTransferInfo
	if err := json.Unmarshal(raw, &info); err != nil {
		slog.Debug("simplex: cannot parse rcvFileDescrReady", "error", err)
		return
	}

	fileID := info.FileID.String()
	if fileID == "" || fileID == "0" {
		return
	}

	// Check if we already requested this download (from handleChatItem).
	c.pendingMu.Lock()
	_, exists := c.pendingFiles[fileID]
	c.pendingMu.Unlock()

	if exists {
		// Already tracking this file, skip duplicate freceive.
		return
	}

	// Auto-accept the file download. It may arrive before newChatItems.
	if _, err := c.sendCommand(fmt.Sprintf("/freceive %s", fileID)); err != nil {
		slog.Warn("simplex: freceive (early) failed", "file_id", fileID, "error", err)
	} else {
		slog.Debug("simplex: early freceive sent", "file_id", fileID)
	}
}

// handleRcvFileComplete handles the file download completion event.
func (c *Channel) handleRcvFileComplete(raw json.RawMessage) {
	if raw == nil {
		return
	}

	// Parse the chat item from the rcvFileComplete event.
	// Structure: { chatItem: { file: { fileId, fileSource: { filePath } } } }
	var wrapper struct {
		ChatItem struct {
			File *chatItemFile `json:"file"`
		} `json:"chatItem"`
	}
	if err := json.Unmarshal(raw, &wrapper); err != nil {
		slog.Debug("simplex: cannot parse rcvFileComplete", "error", err)
		return
	}

	if wrapper.ChatItem.File == nil {
		return
	}

	fileID := wrapper.ChatItem.File.FileID.String()
	filePath := ""
	if wrapper.ChatItem.File.FileSource != nil {
		filePath = strings.TrimSpace(wrapper.ChatItem.File.FileSource.FilePath)
	}

	slog.Debug("simplex: file download complete", "file_id", fileID, "path", filePath)

	// Look up the pending file.
	c.pendingMu.Lock()
	pf, exists := c.pendingFiles[fileID]
	if exists {
		pf.timer.Stop()
		delete(c.pendingFiles, fileID)
	}
	c.pendingMu.Unlock()

	if !exists {
		slog.Debug("simplex: rcvFileComplete for unknown file", "file_id", fileID)
		return
	}

	// Finalize the pending message with the downloaded file.
	c.finalizePendingFile(pf, filePath)
}

// handlePendingTimeout fires when a file download doesn't complete in time.
// Delivers the message without the media attachment.
func (c *Channel) handlePendingTimeout(fileID string) {
	c.pendingMu.Lock()
	pf, exists := c.pendingFiles[fileID]
	if exists {
		delete(c.pendingFiles, fileID)
	}
	c.pendingMu.Unlock()

	if !exists {
		return
	}

	slog.Warn("simplex: file download timed out, delivering without media", "file_id", fileID)
	c.finalizePendingFile(pf, "") // empty path = no media
}

// finalizePendingFile completes processing of a chat item whose file download is done.
func (c *Channel) finalizePendingFile(pf *pendingFile, filePath string) {
	item := pf.item

	senderID, senderName := c.extractSender(item)
	if senderID == "" {
		return
	}

	groupID := item.ChatInfo.GroupInfo.GroupID.String()
	msgType := ""
	if item.ChatItem.Content.MsgContent != nil {
		msgType = item.ChatItem.Content.MsgContent.Type
	}

	// Build content with media tags.
	text := ""
	if item.ChatItem.Content.MsgContent != nil {
		text = item.ChatItem.Content.MsgContent.Text
	}

	var mediaPaths []string

	if filePath != "" {
		switch msgType {
		case "voice", "audio":
			// Transcribe via STT if configured.
			transcript := ""
			slog.Debug("simplex: voice file received, attempting STT",
				"stt_proxy_url", c.config.STTProxyURL,
				"path", filePath,
			)
			if c.config.STTProxyURL != "" {
				var err error
				transcript, err = media.TranscribeAudio(c.ctx, media.STTConfig{
					ProxyURL:       c.config.STTProxyURL,
					APIKey:         c.config.STTAPIKey,
					TimeoutSeconds: c.config.STTTimeoutSeconds,
				}, filePath)
				if err != nil {
					slog.Warn("simplex: STT transcription failed", "error", err, "path", filePath)
				} else if transcript != "" {
					slog.Debug("simplex: voice transcribed",
						"length", len(transcript),
						"preview", truncatePreview(transcript, 80),
					)
				}
			}

			// Build media tag.
			mi := media.MediaInfo{
				Type:       msgType,
				FilePath:   filePath,
				FileName:   item.ChatItem.File.FileName,
				FileSize:   item.ChatItem.File.FileSize,
				Transcript: transcript,
			}
			mediaTag := media.BuildMediaTags([]media.MediaInfo{mi})

			if text != "" {
				text = mediaTag + "\n\n" + text
			} else {
				text = mediaTag
			}

			mediaPaths = append(mediaPaths, filePath)

		case "image":
			mi := media.MediaInfo{
				Type:     media.TypeImage,
				FilePath: filePath,
				FileName: item.ChatItem.File.FileName,
				FileSize: item.ChatItem.File.FileSize,
			}
			mediaTag := media.BuildMediaTags([]media.MediaInfo{mi})
			if text != "" {
				text = mediaTag + "\n\n" + text
			} else {
				text = mediaTag
			}
			mediaPaths = append(mediaPaths, filePath)

		default:
			// Generic file — just note it.
			if text == "" {
				text = fmt.Sprintf("[File: %s]", item.ChatItem.File.FileName)
			}
			mediaPaths = append(mediaPaths, filePath)
		}
	} else if text == "" {
		// No file and no text — nothing to deliver.
		text = "[voice message — download failed]"
	}

	metadata := map[string]string{
		"user_name":  senderName,
		"message_id": item.ChatItem.Meta.ItemID.String(),
		"group_name": item.ChatInfo.GroupInfo.LocalDisplayName,
	}

	slog.Debug("simplex: finalized file message",
		"sender_id", senderID,
		"group_id", groupID,
		"msg_type", msgType,
		"has_file", filePath != "",
		"preview", channels.Truncate(text, 80),
	)

	c.HandleMessage(senderID, groupID, text, mediaPaths, metadata, "group")
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

// mimeFromPath guesses a MIME type from a file extension.
func mimeFromPath(p string) string {
	switch strings.ToLower(filepath.Ext(p)) {
	case ".m4a", ".aac":
		return "audio/aac"
	case ".ogg", ".opus":
		return "audio/ogg"
	case ".mp3":
		return "audio/mpeg"
	case ".wav":
		return "audio/wav"
	case ".jpg", ".jpeg":
		return "image/jpeg"
	case ".png":
		return "image/png"
	case ".gif":
		return "image/gif"
	case ".webp":
		return "image/webp"
	case ".mp4":
		return "video/mp4"
	case ".pdf":
		return "application/pdf"
	default:
		return "application/octet-stream"
	}
}

// truncatePreview returns s truncated to maxLen with "..." suffix if needed.
func truncatePreview(s string, maxLen int) string {
	if len(s) > maxLen {
		return s[:maxLen] + "..."
	}
	return s
}
