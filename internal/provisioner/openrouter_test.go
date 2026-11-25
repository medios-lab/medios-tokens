package provisioner

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestOpenRouterProvisionAndSettleRevoke(t *testing.T) {
	var createdName string
	var createdLimit float64
	var deleted bool

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer prov-secret" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/keys":
			var req struct {
				Name  string  `json:"name"`
				Limit float64 `json:"limit"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			createdName = req.Name
			createdLimit = req.Limit
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"key": "sk-or-v1-ephemeral",
				"data": map[string]any{
					"hash":  "hash-abc123",
					"limit": req.Limit,
				},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/keys/hash-abc123":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": map[string]any{
					"hash":  "hash-abc123",
					"usage": 0.18,
				},
			})
		case r.Method == http.MethodDelete && r.URL.Path == "/api/v1/keys/hash-abc123":
			deleted = true
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	p, err := NewOpenRouter("prov-secret", srv.URL, srv.Client())
	if err != nil {
		t.Fatalf("NewOpenRouter: %v", err)
	}

	grant, err := p.Provision(context.Background(), "task-42", 0.25)
	if err != nil {
		t.Fatalf("Provision: %v", err)
	}
	if createdName != "medios-task-42" || createdLimit != 0.25 {
		t.Fatalf("created (%q, %v), want (medios-task-42, 0.25)", createdName, createdLimit)
	}
	if grant.Hash != "hash-abc123" || grant.Secret != "sk-or-v1-ephemeral" || grant.Limit != 0.25 {
		t.Fatalf("unexpected grant: %+v", grant)
	}

	actual, err := p.SettleAndRevoke(context.Background(), "task-42", grant.Hash)
	if err != nil {
		t.Fatalf("SettleAndRevoke: %v", err)
	}
	if actual != 0.18 {
		t.Fatalf("actual = %v, want 0.18", actual)
	}
	if !deleted {
		t.Fatal("expected DELETE /api/v1/keys/hash-abc123 to be called")
	}
}
