package main

import (
	"strings"
	"testing"
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
