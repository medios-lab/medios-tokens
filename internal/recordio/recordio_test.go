package recordio

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

func TestReaderMultipleFrames(t *testing.T) {
	f1 := `{"type":"SUBSCRIBED"}`
	f2 := `{"type":"HEARTBEAT"}`
	stream := "\n" + "21\n" + f1 + "20\n" + f2

	r := NewReader(strings.NewReader(stream))
	var m1, m2 map[string]string
	if err := r.Next(&m1); err != nil {
		t.Fatalf("frame 1: %v", err)
	}
	if m1["type"] != "SUBSCRIBED" {
		t.Fatalf("got %q, want SUBSCRIBED", m1["type"])
	}
	if err := r.Next(&m2); err != nil {
		t.Fatalf("frame 2: %v", err)
	}
	if m2["type"] != "HEARTBEAT" {
		t.Fatalf("got %q, want HEARTBEAT", m2["type"])
	}
	if err := r.Next(&m2); err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
}

func TestReaderTruncatedFrame(t *testing.T) {
	r := NewReader(bytes.NewBufferString("50\n{\"type\":\"SHORT\"}"))
	var m map[string]any
	if err := r.Next(&m); err == nil {
		t.Fatal("expected error on truncated frame")
	}
}
