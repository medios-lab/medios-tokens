package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/medios-lab/medios-tokens/internal/ledger"
)

func writeRecord(w io.Writer, flusher http.Flusher, v any) {
	b, _ := json.Marshal(v)
	_, _ = fmt.Fprintf(w, "%d\n%s", len(b), b)
	flusher.Flush()
}

func TestProviderReserveAuthorizeSettleRefill(t *testing.T) {
	var mu sync.Mutex
	var advertised []float64
	var publishAcks int
	ctrlReady := make(chan struct{})
	emitTask := make(chan struct{})
	done := make(chan struct{})

	nodeSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var call map[string]any
		_ = json.NewDecoder(r.Body).Decode(&call)
		switch call["type"] {
		case "SUBSCRIBE":
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Medios-Stream-Id", "stream-1")
			flusher := w.(http.Flusher)
			writeRecord(w, flusher, map[string]any{
				"type":       "SUBSCRIBED",
				"subscribed": map[string]any{"provider_id": map[string]any{"value": "rp-test"}},
			})
			// Simulate medios-node sending PUBLISH_RESOURCES twice for one task.
			writeRecord(w, flusher, map[string]any{
				"type":              "PUBLISH_RESOURCES",
				"publish_resources": map[string]any{"uuid": map[string]any{"value": "u1"}},
			})
			writeRecord(w, flusher, map[string]any{
				"type":              "PUBLISH_RESOURCES",
				"publish_resources": map[string]any{"uuid": map[string]any{"value": "u2"}},
			})
			<-done
		case "UPDATE_STATE":
			us := call["update_state"].(map[string]any)
			res := us["resources"].([]any)
			val := 0.0
			if len(res) > 0 {
				val = res[0].(map[string]any)["scalar"].(map[string]any)["value"].(float64)
			}
			mu.Lock()
			advertised = append(advertised, val)
			mu.Unlock()
			w.WriteHeader(http.StatusAccepted)
		case "UPDATE_PUBLISH_RESOURCES_STATUS":
			mu.Lock()
			publishAcks++
			mu.Unlock()
			w.WriteHeader(http.StatusAccepted)
		}
	}))
	defer nodeSrv.Close()

	ctrlSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		flusher := w.(http.Flusher)
		close(ctrlReady)
		<-emitTask
		writeRecord(w, flusher, map[string]any{
			"type": "TASK_ADDED",
			"task_added": map[string]any{
				"task": map[string]any{
					"task_id": map[string]any{"value": "task-1"},
					"resources": []map[string]any{
						{
							"name":        "token_budget",
							"type":        "SCALAR",
							"provider_id": map[string]any{"value": "rp-test"},
							"scalar":      map[string]any{"value": 25.0},
						},
					},
				},
			},
		})
		<-done
	}))
	defer ctrlSrv.Close()

	l, err := ledger.Open("", "token_budget", 100, 100)
	if err != nil {
		t.Fatal(err)
	}
	p := New(Config{
		ControllerURL: ctrlSrv.URL,
		NodeURL:       nodeSrv.URL,
		ResourceName:  "token_budget",
		RefillAmount:  10,
	}, l, log.New(io.Discard, "", 0))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = p.Run(ctx) }()

	select {
	case <-ctrlReady:
	case <-time.After(3 * time.Second):
		t.Fatal("controller subscriber did not connect")
	}

	close(emitTask)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		ok := publishAcks == 2 && len(advertised) >= 2
		mu.Unlock()
		if ok {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Settle task-1 with actual=18 -> releases 7 unused back to 82.
	adminSrv := httptest.NewServer(p.AdminHandler())
	defer adminSrv.Close()
	resp, err := http.Post(adminSrv.URL+"/settle", "application/json", bytes.NewBufferString(`{"task_id":"task-1","actual":18}`))
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("settle failed: err=%v resp=%v", err, resp)
	}
	_ = resp.Body.Close()

	// Refill +10 -> 92.
	resp, err = http.Post(adminSrv.URL+"/refill", "application/json", bytes.NewBufferString(`{"amount":10}`))
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("refill failed: err=%v resp=%v", err, resp)
	}
	_ = resp.Body.Close()

	close(done)
	cancel()

	mu.Lock()
	defer mu.Unlock()
	if publishAcks != 2 {
		t.Fatalf("publishAcks = %d, want 2", publishAcks)
	}
	want := []float64{100, 75, 82, 92}
	if len(advertised) != len(want) {
		t.Fatalf("advertised = %v, want %v", advertised, want)
	}
	for i := range want {
		if advertised[i] != want[i] {
			t.Fatalf("advertised[%d] = %v, want %v (full: %v)", i, advertised[i], want[i], advertised)
		}
	}
}
