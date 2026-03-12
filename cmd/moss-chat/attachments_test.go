package main

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"

	"moss/internal/gossip"
)

func TestAttachmentProgressBar(t *testing.T) {
	bar := attachmentProgressBar(2, 4)
	if bar != "[========--------]" {
		t.Fatalf("unexpected progress bar %q", bar)
	}
}

func TestRenderRoomLinesWrapsActionRegions(t *testing.T) {
	rendered := renderRoomLines([]chatLine{
		{Text: "[12:00:00] plain line"},
		{Text: "[12:00:01] file offer", RegionID: "in-1"},
	})
	if !strings.Contains(rendered, `["in-1"]`) {
		t.Fatalf("expected rendered text to include region tag, got %q", rendered)
	}
	if !strings.Contains(rendered, "plain line") {
		t.Fatalf("expected rendered text to include plain line, got %q", rendered)
	}
}

func TestAttachmentChunkFitsUDPEnvelopeBudget(t *testing.T) {
	raw := bytesRepeat('a', attachmentChunkSize)
	payload, err := json.Marshal(chatPayload{
		Kind:       "attachment-chunk",
		TransferID: "x",
		FileName:   "demo.bin",
		FileSize:   int64(len(raw)),
		FileSHA256: strings.Repeat("a", 64),
		ChunkIndex: 0,
		ChunkCount: 1,
		ChunkData:  base64.StdEncoding.EncodeToString(raw),
		Attachment: true,
	})
	if err != nil {
		t.Fatalf("marshal chat payload: %v", err)
	}
	envelope, err := json.Marshal(gossip.Envelope{
		Type:    gossip.TypePublish,
		Channel: "lobby",
		Payload: payload,
	})
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	if len(envelope) > 1400 {
		t.Fatalf("attachment envelope too large for udp-friendly path: %d bytes", len(envelope))
	}
}

func bytesRepeat(ch byte, count int) []byte {
	out := make([]byte, count)
	for i := range out {
		out[i] = ch
	}
	return out
}
