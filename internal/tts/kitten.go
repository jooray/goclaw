package tts

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

// KittenProvider implements TTS via kitten-tts (local, CPU-only).
// Requires kitten-tts.sh wrapper with a uv-managed virtualenv.
// Output is always 24 kHz WAV.
type KittenProvider struct {
	wrapperPath string // path to kitten-tts.sh
	voice       string // default "Rosie"
	speed       string // speech speed, e.g. "1.6"
	timeoutMs   int
}

// KittenConfig configures the kitten-tts provider.
type KittenConfig struct {
	WrapperPath string // path to kitten-tts.sh (required)
	Voice       string // default "Rosie"
	Speed       string // default "1.5"
	TimeoutMs   int
}

// NewKittenProvider creates a kitten-tts provider.
func NewKittenProvider(cfg KittenConfig) *KittenProvider {
	p := &KittenProvider{
		wrapperPath: cfg.WrapperPath,
		voice:       cfg.Voice,
		speed:       cfg.Speed,
		timeoutMs:   cfg.TimeoutMs,
	}
	if p.voice == "" {
		p.voice = "Rosie"
	}
	if p.speed == "" {
		p.speed = "1.5"
	}
	if p.timeoutMs <= 0 {
		p.timeoutMs = 120000 // kitten-tts on a slow CPU can take a while
	}
	return p
}

func (p *KittenProvider) Name() string { return "kitten" }

// Synthesize runs kitten-tts.sh to generate WAV audio.
func (p *KittenProvider) Synthesize(ctx context.Context, text string, _ Options) (*SynthResult, error) {
	if p.wrapperPath == "" {
		return nil, fmt.Errorf("kitten-tts: wrapper_path not configured")
	}

	// Create temp file for output
	tmpDir := os.TempDir()
	outPath := filepath.Join(tmpDir, fmt.Sprintf("kitten-tts-%d.wav", time.Now().UnixNano()))
	defer os.Remove(outPath)

	// Write text to a temp file to avoid shell argument length issues
	textFile := filepath.Join(tmpDir, fmt.Sprintf("kitten-tts-%d.txt", time.Now().UnixNano()))
	if err := os.WriteFile(textFile, []byte(text), 0644); err != nil {
		return nil, fmt.Errorf("kitten-tts: write text file: %w", err)
	}
	defer os.Remove(textFile)

	args := []string{
		"--file", textFile,
		outPath,
		"--voice", p.voice,
		"--speed", p.speed,
	}

	timeout := time.Duration(p.timeoutMs) * time.Millisecond
	cmdCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cmd := exec.CommandContext(cmdCtx, p.wrapperPath, args...)
	if output, err := cmd.CombinedOutput(); err != nil {
		return nil, fmt.Errorf("kitten-tts failed: %w (output: %s)", err, string(output))
	}

	audio, err := os.ReadFile(outPath)
	if err != nil {
		return nil, fmt.Errorf("read kitten-tts output: %w", err)
	}

	return &SynthResult{
		Audio:     audio,
		Extension: "wav",
		MimeType:  "audio/wav",
	}, nil
}
