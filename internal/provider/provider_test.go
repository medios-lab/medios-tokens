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
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/medios-lab/medios-tokens/internal/ledger"
	"github.com/medios-lab/medios-tokens/internal/provisioner"
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
	p, err := New(Config{
		ControllerURL: ctrlSrv.URL,
		NodeURL:       nodeSrv.URL,
		Pools:         map[string]PoolOptions{"token_budget": {RefillAmount: 10}},
	}, l, log.New(io.Discard, "", 0))
	if err != nil {
		t.Fatal(err)
	}

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

type fakeProvisioner struct {
	mu      sync.Mutex
	actual  float64
	revoked []string
}

func (f *fakeProvisioner) Provision(_ context.Context, taskID string, limit float64) (provisioner.KeyGrant, error) {
	return provisioner.KeyGrant{
		Hash:   "hash-" + taskID,
		Secret: "sk-or-secret-" + taskID,
		Limit:  limit,
	}, nil
}

func (f *fakeProvisioner) SettleAndRevoke(_ context.Context, _ string, keyHash string) (float64, error) {
	f.mu.Lock()
	f.revoked = append(f.revoked, keyHash)
	f.mu.Unlock()
	return f.actual, nil
}

func TestProviderScopedKeyProvisionAndTerminalSettle(t *testing.T) {
	var mu sync.Mutex
	var advertised []float64
	ctrlReady := make(chan struct{})
	emitTask := make(chan struct{})
	emitTerminal := make(chan struct{})
	done := make(chan struct{})

	nodeSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var call map[string]any
		_ = json.NewDecoder(r.Body).Decode(&call)
		switch call["type"] {
		case "SUBSCRIBE":
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Medios-Stream-Id", "stream-2")
			flusher := w.(http.Flusher)
			writeRecord(w, flusher, map[string]any{
				"type":       "SUBSCRIBED",
				"subscribed": map[string]any{"provider_id": map[string]any{"value": "rp-test"}},
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
					"task_id": map[string]any{"value": "task-k1"},
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
		<-emitTerminal
		writeRecord(w, flusher, map[string]any{
			"type": "TASK_UPDATED",
			"task_updated": map[string]any{
				"state": "TASK_KILLED",
				"status": map[string]any{
					"task_id": map[string]any{"value": "task-k1"},
					"state":   "TASK_KILLED",
				},
			},
		})
		<-done
	}))
	defer ctrlSrv.Close()

	stateFile := filepath.Join(t.TempDir(), "ledger.json")
	l, err := ledger.Open(stateFile, "token_budget", 100, 100)
	if err != nil {
		t.Fatal(err)
	}
	fp := &fakeProvisioner{actual: 6.5}
	p, err := New(Config{
		ControllerURL: ctrlSrv.URL,
		NodeURL:       nodeSrv.URL,
		Provisioner:   fp,
	}, l, log.New(io.Discard, "", 0))
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = p.Run(ctx) }()

	select {
	case <-ctrlReady:
	case <-time.After(3 * time.Second):
		t.Fatal("controller subscriber did not connect")
	}

	adminSrv := httptest.NewServer(p.AdminHandler())
	defer adminSrv.Close()

	// 1. Provision a per-task scoped key before launching task-k1.
	resp, err := http.Post(adminSrv.URL+"/provision", "application/json", bytes.NewBufferString(`{"task_id":"task-k1","limit":25}`))
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("provision failed: err=%v resp=%v", err, resp)
	}
	var grant map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&grant); err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if grant["key_hash"] != "hash-task-k1" || grant["key"] != "sk-or-secret-task-k1" {
		t.Fatalf("unexpected grant: %+v", grant)
	}

	// Verify checkpoint stores key_hash and NEVER stores the raw secret.
	rawCheckpoint, err := os.ReadFile(stateFile)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(rawCheckpoint), "hash-task-k1") {
		t.Fatalf("checkpoint missing key_hash: %s", string(rawCheckpoint))
	}
	if strings.Contains(string(rawCheckpoint), "sk-or-secret-task-k1") {
		t.Fatalf("checkpoint leaked raw secret key!")
	}

	// 2. Task launches -> debit 25 (100 -> 75).
	close(emitTask)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(advertised)
		mu.Unlock()
		if n >= 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	// 3. Task is killed -> provider automatically settles actual=6.5 and revokes key (75 + 18.5 = 93.5).
	close(emitTerminal)
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(advertised)
		mu.Unlock()
		if n >= 3 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	close(done)
	cancel()

	mu.Lock()
	defer mu.Unlock()
	want := []float64{100, 75, 93.5}
	if len(advertised) != len(want) {
		t.Fatalf("advertised = %v, want %v", advertised, want)
	}
	for i := range want {
		if advertised[i] != want[i] {
			t.Fatalf("advertised[%d] = %v, want %v (full: %v)", i, advertised[i], want[i], advertised)
		}
	}
	fp.mu.Lock()
	defer fp.mu.Unlock()
	if len(fp.revoked) != 1 || fp.revoked[0] != "hash-task-k1" {
		t.Fatalf("revoked = %v, want [hash-task-k1]", fp.revoked)
	}
}
