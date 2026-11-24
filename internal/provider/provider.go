// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package provider

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/medios-lab/medios-tokens/internal/ledger"
	"github.com/medios-lab/medios-tokens/internal/recordio"
)

type Config struct {
	ControllerURL  string
	NodeURL        string
	ProviderName   string
	ProviderType   string
	ResourceName   string
	RefillAmount   float64
	RefillInterval time.Duration
	AdminAddr      string
}

type Provider struct {
	cfg    Config
	ledger *ledger.Ledger
	client *http.Client
	logger *log.Logger

	mu       sync.Mutex
	streamID string
}

func New(cfg Config, l *ledger.Ledger, logger *log.Logger) *Provider {
	if cfg.ProviderType == "" {
		cfg.ProviderType = "org.medios.rp.tokens"
	}
	if cfg.ProviderName == "" {
		cfg.ProviderName = "medios-tokens"
	}
	if cfg.ResourceName == "" {
		cfg.ResourceName = "token_budget"
	}
	if logger == nil {
		logger = log.Default()
	}
	return &Provider{
		cfg:    cfg,
		ledger: l,
		client: &http.Client{Timeout: 15 * time.Second},
		logger: logger,
	}
}

func randomUUIDBase64() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return base64.StdEncoding.EncodeToString(b)
}

type idVal struct {
	Value string `json:"value"`
}

type rpEvent struct {
	Type       string `json:"type"`
	Subscribed *struct {
		ProviderID idVal `json:"provider_id"`
	} `json:"subscribed"`
	PublishResources *struct {
		UUID idVal `json:"uuid"`
	} `json:"publish_resources"`
}

type ctrlEvent struct {
	Type      string `json:"type"`
	TaskAdded *struct {
		Task struct {
			TaskID    idVal `json:"task_id"`
			Resources []struct {
				Name       string `json:"name"`
				Type       string `json:"type"`
				ProviderID *idVal `json:"provider_id"`
				Scalar     *struct {
					Value float64 `json:"value"`
				} `json:"scalar"`
			} `json:"resources"`
		} `json:"task"`
	} `json:"task_added"`
}

func (p *Provider) postJSON(ctx context.Context, url string, payload any, streamID string) (int, string, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return 0, "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return 0, "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if streamID != "" {
		req.Header.Set("Medios-Stream-Id", streamID)
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(raw), nil
}

// Advertise pushes the ledger's current balance to medios-node via UPDATE_STATE.
func (p *Provider) Advertise(ctx context.Context, reason string) error {
	p.mu.Lock()
	sid := p.streamID
	p.mu.Unlock()
	pid := p.ledger.ProviderID()
	if pid == "" || sid == "" {
		return nil
	}

	bal := p.ledger.Balance()
	resources := []map[string]any{}
	if bal > 0 {
		resources = append(resources, map[string]any{
			"name":        p.cfg.ResourceName,
			"type":        "SCALAR",
			"scalar":      map[string]any{"value": bal},
			"provider_id": map[string]any{"value": pid},
		})
	}

	call := map[string]any{
		"type":                 "UPDATE_STATE",
		"resource_provider_id": map[string]any{"value": pid},
		"update_state": map[string]any{
			"resources":             resources,
			"resource_version_uuid": map[string]any{"value": randomUUIDBase64()},
			"operations":            []any{},
		},
	}

	endpoint := strings.TrimRight(p.cfg.NodeURL, "/") + "/api/v1/resource_provider"
	st, body, err := p.postJSON(ctx, endpoint, call, sid)
	if err != nil {
		return err
	}
	if st != http.StatusAccepted {
		return fmt.Errorf("UPDATE_STATE returned HTTP %d: %s", st, body)
	}
	p.logger.Printf("UPDATE_STATE %s=%.6g (%s)", p.cfg.ResourceName, bal, reason)
	return nil
}

// Refill tops up the ledger by amount (up to cap) and advertises if changed.
func (p *Provider) Refill(ctx context.Context, amount float64, source string) (float64, error) {
	bal, changed, err := p.ledger.Refill(amount)
	if err != nil {
		return bal, err
	}
	if changed {
		if err := p.Advertise(ctx, fmt.Sprintf("refill +%.6g via %s", amount, source)); err != nil {
			return bal, err
		}
	}
	return bal, nil
}

// Settle reconciles actual consumption for taskID, releases unused credit back
// to the balance, and advertises the updated capacity.
func (p *Provider) Settle(ctx context.Context, taskID string, actual float64) (float64, float64, error) {
	delta, bal, err := p.ledger.SettleTask(taskID, actual)
	if err != nil {
		return delta, bal, err
	}
	if delta != 0 {
		if err := p.Advertise(ctx, fmt.Sprintf("settled task %s actual=%.6g delta=%+.6g", taskID, actual, delta)); err != nil {
			return delta, bal, err
		}
	}
	return delta, bal, nil
}

// AdminHandler exposes tiny local endpoints for inspecting balance, settling
// actual spend, and triggering external refills:
//   GET  /balance
//   POST /settle  {"task_id":"...", "actual": 18.0}
//   POST /refill  {"amount": 40.0}
func (p *Provider) AdminHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/balance", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(p.ledger.Snapshot())
	})
	mux.HandleFunc("/settle", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			TaskID string  `json:"task_id"`
			Actual float64 `json:"actual"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.TaskID == "" {
			http.Error(w, "expecting JSON {\"task_id\": \"...\", \"actual\": <number>}", http.StatusBadRequest)
			return
		}
		delta, bal, err := p.Settle(r.Context(), req.TaskID, req.Actual)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"task_id": req.TaskID, "delta": delta, "balance": bal})
	})
	mux.HandleFunc("/refill", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Amount float64 `json:"amount"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Amount <= 0 {
			req.Amount = p.cfg.RefillAmount
		}
		bal, err := p.Refill(r.Context(), req.Amount, "http")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"balance": bal})
	})
	return mux
}

func (p *Provider) watchController(ctx context.Context) error {
	endpoint := strings.TrimRight(p.cfg.ControllerURL, "/") + "/api/v1"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader([]byte(`{"type":"SUBSCRIBE"}`)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	streamClient := &http.Client{}
	resp, err := streamClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("controller SUBSCRIBE returned HTTP %d: %s", resp.StatusCode, string(raw))
	}

	reader := recordio.NewReader(resp.Body)
	for {
		var ev ctrlEvent
		if err := reader.Next(&ev); err != nil {
			return err
		}
		if ev.Type != "TASK_ADDED" || ev.TaskAdded == nil {
			continue
		}
		taskID := ev.TaskAdded.Task.TaskID.Value
		pid := p.ledger.ProviderID()
		var spent float64
		for _, res := range ev.TaskAdded.Task.Resources {
			if res.Name == p.cfg.ResourceName && res.Type == "SCALAR" &&
				res.ProviderID != nil && res.ProviderID.Value == pid && res.Scalar != nil {
				spent += res.Scalar.Value
			}
		}
		if spent <= 0 {
			continue
		}
		_, changed, err := p.ledger.DebitTask(taskID, spent)
		if err != nil {
			p.logger.Printf("debit error for task %s: %v", taskID, err)
			continue
		}
		if changed {
			if err := p.Advertise(ctx, fmt.Sprintf("debited %.6g for task %s", spent, taskID)); err != nil {
				p.logger.Printf("advertise error after debit %s: %v", taskID, err)
			}
		}
	}
}

// Run subscribes to the node Resource Provider API and controller event stream
// until ctx is cancelled or the provider stream terminates.
func (p *Provider) Run(ctx context.Context) error {
	info := map[string]any{
		"type": p.cfg.ProviderType,
		"name": p.cfg.ProviderName,
		"default_reservations": []map[string]any{
			{"type": "STATIC", "role": "*"},
		},
	}
	if existingID := p.ledger.ProviderID(); existingID != "" {
		info["id"] = map[string]any{"value": existingID}
	}

	subBody, err := json.Marshal(map[string]any{
		"type":      "SUBSCRIBE",
		"subscribe": map[string]any{"resource_provider_info": info},
	})
	if err != nil {
		return err
	}

	endpoint := strings.TrimRight(p.cfg.NodeURL, "/") + "/api/v1/resource_provider"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(subBody))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	streamClient := &http.Client{}
	resp, err := streamClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("resource_provider SUBSCRIBE returned HTTP %d: %s", resp.StatusCode, string(raw))
	}

	p.mu.Lock()
	p.streamID = resp.Header.Get("Medios-Stream-Id")
	p.mu.Unlock()

	subCtx, cancelSub := context.WithCancel(ctx)
	defer cancelSub()

	reader := recordio.NewReader(resp.Body)
	started := false
	for {
		var ev rpEvent
		if err := reader.Next(&ev); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}

		switch ev.Type {
		case "SUBSCRIBED":
			if ev.Subscribed == nil {
				continue
			}
			pid := ev.Subscribed.ProviderID.Value
			if err := p.ledger.SetProviderID(pid); err != nil {
				return fmt.Errorf("checkpoint provider_id: %w", err)
			}
			p.logger.Printf("subscribed provider_id=%s resource=%s balance=%.6g", pid, p.cfg.ResourceName, p.ledger.Balance())
			if err := p.Advertise(subCtx, "initial"); err != nil {
				return err
			}
			if !started {
				started = true
				go func() {
					if err := p.watchController(subCtx); err != nil && subCtx.Err() == nil {
						p.logger.Printf("controller watcher exited: %v", err)
					}
				}()
				if p.cfg.RefillInterval > 0 && p.cfg.RefillAmount > 0 {
					go func() {
						ticker := time.NewTicker(p.cfg.RefillInterval)
						defer ticker.Stop()
						for {
							select {
							case <-subCtx.Done():
								return
							case <-ticker.C:
								if _, err := p.Refill(subCtx, p.cfg.RefillAmount, "timer"); err != nil && subCtx.Err() == nil {
									p.logger.Printf("timer refill failed: %v", err)
								}
							}
						}
					}()
				}
			}

		case "PUBLISH_RESOURCES":
			if ev.PublishResources == nil {
				continue
			}
			status := "OK"
			if !p.ledger.CanAuthorize() {
				status = "FAILED"
			}
			p.mu.Lock()
			sid := p.streamID
			p.mu.Unlock()
			ack := map[string]any{
				"type":                 "UPDATE_PUBLISH_RESOURCES_STATUS",
				"resource_provider_id": map[string]any{"value": p.ledger.ProviderID()},
				"update_publish_resources_status": map[string]any{
					"uuid":   map[string]any{"value": ev.PublishResources.UUID.Value},
					"status": status,
				},
			}
			st, body, err := p.postJSON(subCtx, endpoint, ack, sid)
			if err != nil || st != http.StatusAccepted {
				p.logger.Printf("UPDATE_PUBLISH_RESOURCES_STATUS error (http=%d err=%v): %s", st, err, body)
			}
		}
	}
}
