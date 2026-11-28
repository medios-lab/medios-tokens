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
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/medios-lab/medios-tokens/internal/ledger"
	"github.com/medios-lab/medios-tokens/internal/provisioner"
	"github.com/medios-lab/medios-tokens/internal/recordio"
)

// maxSyncAttempts is the number of external reads one sync of a pool may
// make. Heuristic: a reading is discarded when a task in the pool settled
// while the read was in flight, and one immediate re-read usually lands in a
// quiet window; a second discard waits for the next interval so a busy pool
// cannot multiply requests against the external endpoint.
const maxSyncAttempts = 2

// errSyncedPool is returned when a refill targets a pool whose balance is
// read from an external endpoint; the next read would overwrite the refill.
var errSyncedPool = errors.New("pool balance is synced from an external endpoint and cannot be refilled")

// BalanceSource reads a pool's balance from the system that owns its quota.
type BalanceSource interface {
	Fetch(ctx context.Context) (ledger.External, error)
}

// PoolOptions configures the provider's handling of one ledger pool.
type PoolOptions struct {
	// RefillAmount is added to the pool on each refill trigger (SIGUSR1,
	// timer, or a /refill request without an amount). Zero disables triggers
	// for the pool.
	RefillAmount float64
	// Source, when set, makes the pool's balance follow an external reading
	// taken once before subscribing and then every SyncInterval.
	Source       BalanceSource
	SyncInterval time.Duration
}

type Config struct {
	ControllerURL string
	NodeURL       string
	ProviderName  string
	ProviderType  string
	// Pools holds options keyed by ledger pool name. A ledger pool without
	// an entry gets the zero PoolOptions.
	Pools          map[string]PoolOptions
	RefillInterval time.Duration
	AdminAddr      string
	Provisioner    provisioner.Provisioner
	// ProvisionerResource names the pool whose tasks the Provisioner mints
	// keys for and settles. It may be empty only when the ledger has one pool.
	ProvisionerResource string
}

type Provider struct {
	cfg       Config
	ledger    *ledger.Ledger
	resources []string
	client    *http.Client
	logger    *log.Logger

	mu       sync.Mutex
	streamID string

	// advMu serializes Advertise so the balances read from the ledger and
	// the UPDATE_STATE that carries them reach the node in the same order.
	advMu sync.Mutex
	// advPending is true while the node may hold balances older than the
	// ledger's: the most recent UPDATE_STATE failed. Guarded by advMu.
	advPending bool
}

func New(cfg Config, l *ledger.Ledger, logger *log.Logger) (*Provider, error) {
	if cfg.ProviderType == "" {
		cfg.ProviderType = "org.medios.rp.tokens"
	}
	if cfg.ProviderName == "" {
		cfg.ProviderName = "medios-tokens"
	}
	if logger == nil {
		logger = log.Default()
	}
	p := &Provider{
		cfg:       cfg,
		ledger:    l,
		resources: l.Resources(),
		client:    &http.Client{Timeout: 15 * time.Second},
		logger:    logger,
	}
	for name, opts := range cfg.Pools {
		if !p.hasPool(name) {
			return nil, fmt.Errorf("options given for pool %q, which is not in the ledger", name)
		}
		if opts.RefillAmount < 0 {
			return nil, fmt.Errorf("pool %q: refill amount must be non-negative", name)
		}
		if opts.Source != nil && opts.SyncInterval <= 0 {
			return nil, fmt.Errorf("pool %q: sync interval must be positive", name)
		}
		if opts.Source != nil && opts.RefillAmount > 0 {
			return nil, fmt.Errorf("pool %q: %w", name, errSyncedPool)
		}
	}
	if cfg.Provisioner != nil {
		resource, err := p.resolveResource(cfg.ProvisionerResource)
		if err != nil {
			return nil, fmt.Errorf("provisioner pool: %w", err)
		}
		p.cfg.ProvisionerResource = resource
	}
	return p, nil
}

func (p *Provider) hasPool(name string) bool {
	i := sort.SearchStrings(p.resources, name)
	return i < len(p.resources) && p.resources[i] == name
}

// resolveResource maps a request's optional resource name to a pool. An empty
// name selects the pool only when the ledger has exactly one.
func (p *Provider) resolveResource(name string) (string, error) {
	if name == "" {
		if len(p.resources) == 1 {
			return p.resources[0], nil
		}
		return "", fmt.Errorf("\"resource\" is required: provider has %d pools (%s)",
			len(p.resources), strings.Join(p.resources, ", "))
	}
	if !p.hasPool(name) {
		return "", fmt.Errorf("unknown resource %q (pools: %s)", name, strings.Join(p.resources, ", "))
	}
	return name, nil
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
		UUID      idVal `json:"uuid"`
		Resources []struct {
			Name string `json:"name"`
		} `json:"resources"`
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
	TaskUpdated *struct {
		State  string `json:"state"`
		Status *struct {
			TaskID idVal  `json:"task_id"`
			State  string `json:"state"`
		} `json:"status"`
	} `json:"task_updated"`
}

func isTerminalTaskState(state string) bool {
	switch state {
	case "TASK_FINISHED", "TASK_FAILED", "TASK_KILLED", "TASK_LOST",
		"TASK_GONE", "TASK_GONE_BY_OPERATOR", "TASK_ERROR", "TASK_DROPPED":
		return true
	default:
		return false
	}
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

// formatBalances renders pool balances in name order for log lines.
func (p *Provider) formatBalances(balances map[string]float64) string {
	parts := make([]string, 0, len(p.resources))
	for _, name := range p.resources {
		parts = append(parts, fmt.Sprintf("%s=%.6g", name, balances[name]))
	}
	return strings.Join(parts, " ")
}

// Advertise pushes every pool's current balance to medios-node in one
// UPDATE_STATE: one scalar resource per pool with a positive balance.
func (p *Provider) Advertise(ctx context.Context, reason string) error {
	p.advMu.Lock()
	defer p.advMu.Unlock()

	p.mu.Lock()
	sid := p.streamID
	p.mu.Unlock()
	pid := p.ledger.ProviderID()
	if pid == "" || sid == "" {
		return nil
	}

	balances := p.ledger.Balances()
	resources := []map[string]any{}
	for _, name := range p.resources {
		if bal := balances[name]; bal > 0 {
			resources = append(resources, map[string]any{
				"name":        name,
				"type":        "SCALAR",
				"scalar":      map[string]any{"value": bal},
				"provider_id": map[string]any{"value": pid},
			})
		}
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
		p.advPending = true
		return err
	}
	if st != http.StatusAccepted {
		p.advPending = true
		return fmt.Errorf("UPDATE_STATE returned HTTP %d: %s", st, body)
	}
	p.advPending = false
	p.logger.Printf("UPDATE_STATE %s (%s)", p.formatBalances(balances), reason)
	return nil
}

// Refill tops up one pool by amount (up to its cap) and advertises if changed.
func (p *Provider) Refill(ctx context.Context, resource string, amount float64, source string) (float64, error) {
	if p.cfg.Pools[resource].Source != nil {
		return 0, fmt.Errorf("pool %q: %w", resource, errSyncedPool)
	}
	bal, changed, err := p.ledger.Refill(resource, amount)
	if err != nil {
		return bal, err
	}
	if changed {
		if err := p.Advertise(ctx, fmt.Sprintf("refill %s +%.6g via %s", resource, amount, source)); err != nil {
			return bal, err
		}
	}
	return bal, nil
}

// RefillAll applies each pool's configured RefillAmount. Pools with a zero
// amount are left alone.
func (p *Provider) RefillAll(ctx context.Context, source string) error {
	var errs []error
	for _, name := range p.resources {
		amount := p.cfg.Pools[name].RefillAmount
		if amount <= 0 {
			continue
		}
		if _, err := p.Refill(ctx, name, amount, source); err != nil {
			errs = append(errs, fmt.Errorf("pool %q: %w", name, err))
		}
	}
	return errors.Join(errs...)
}

func (p *Provider) hasRefillAmount() bool {
	for _, opts := range p.cfg.Pools {
		if opts.RefillAmount > 0 {
			return true
		}
	}
	return false
}

// Settle reconciles actual consumption for taskID in one pool, releases
// unused credit back to the balance, and advertises the updated capacity.
func (p *Provider) Settle(ctx context.Context, resource, taskID string, actual float64) (float64, float64, error) {
	delta, bal, err := p.ledger.SettleTask(resource, taskID, actual)
	if err != nil {
		return delta, bal, err
	}
	if delta != 0 {
		if err := p.Advertise(ctx, fmt.Sprintf("settled task %s in %s actual=%.6g delta=%+.6g", taskID, resource, actual, delta)); err != nil {
			return delta, bal, err
		}
	}
	return delta, bal, nil
}

// advertisePending reports whether the last UPDATE_STATE failed.
func (p *Provider) advertisePending() bool {
	p.advMu.Lock()
	defer p.advMu.Unlock()
	return p.advPending
}

// syncPool reads one pool's external balance, applies it to the ledger and
// advertises if the pool changed. It also advertises when the previous
// UPDATE_STATE failed, whatever this read returned, so the node does not keep
// balances older than the ledger's after a failed call.
func (p *Provider) syncPool(ctx context.Context, resource string, src BalanceSource) {
	reason, changed := p.reconcilePool(ctx, resource, src)
	if !changed {
		if !p.advertisePending() {
			return
		}
		reason = "retry after failed UPDATE_STATE"
	}
	if err := p.Advertise(ctx, reason); err != nil && ctx.Err() == nil {
		p.logger.Printf("advertise error after sync of %s: %v", resource, err)
	}
}

// reconcilePool reads one pool's external balance and applies it to the
// ledger. A failed read or an unusable response leaves the pool as it was.
// It returns whether the pool changed and the reason to advertise with.
func (p *Provider) reconcilePool(ctx context.Context, resource string, src BalanceSource) (string, bool) {
	for attempt := 1; attempt <= maxSyncAttempts; attempt++ {
		epoch := p.ledger.SettleEpoch(resource)
		reading, err := src.Fetch(ctx)
		if err != nil {
			if ctx.Err() == nil {
				bal, _ := p.ledger.Balance(resource)
				p.logger.Printf("balance sync for %s failed, keeping balance %.6g: %v", resource, bal, err)
			}
			return "", false
		}
		changed, err := p.ledger.Reconcile(resource, epoch, reading)
		if errors.Is(err, ledger.ErrStaleReading) {
			continue
		}
		if err != nil {
			p.logger.Printf("balance sync for %s not applied: %v", resource, err)
			return "", false
		}
		return fmt.Sprintf("synced %s external available=%.6g", resource, reading.Available), changed
	}
	p.logger.Printf("balance sync for %s discarded %d readings taken across task settlements; retrying next interval",
		resource, maxSyncAttempts)
	return "", false
}

// writeJSON sends v as a JSON response body.
func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

// AdminHandler exposes tiny local endpoints for inspecting balances, settling
// actual spend, and triggering external refills:
//
//	GET  /balance
//	POST /settle  {"task_id":"...", "actual": 18.0, "resource": "..."}
//	POST /refill  {"amount": 40.0, "resource": "..."}
//
// "resource" names the pool. It may be omitted only when the provider has
// one pool.
func (p *Provider) AdminHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/balance", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		snap := p.ledger.Snapshot()
		out := map[string]any{"version": snap.Version, "pools": snap.Pools}
		if snap.ProviderID != "" {
			out["provider_id"] = snap.ProviderID
		}
		// With one pool, its fields are also returned at the top level, the
		// shape single-pool clients read.
		if len(snap.Pools) == 1 {
			for name, pool := range snap.Pools {
				out["resource"] = name
				out["balance"] = pool.Balance
				out["cap"] = pool.Cap
				out["tasks"] = pool.Tasks
				if pool.Frozen {
					out["frozen"] = true
				}
			}
		}
		writeJSON(w, out)
	})
	mux.HandleFunc("/settle", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			TaskID   string  `json:"task_id"`
			Actual   float64 `json:"actual"`
			Resource string  `json:"resource"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.TaskID == "" {
			http.Error(w, "expecting JSON {\"task_id\": \"...\", \"actual\": <number>, \"resource\": \"<pool>\"}", http.StatusBadRequest)
			return
		}
		resource, err := p.resolveResource(req.Resource)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		delta, bal, err := p.Settle(r.Context(), resource, req.TaskID, req.Actual)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, map[string]any{"task_id": req.TaskID, "resource": resource, "delta": delta, "balance": bal})
	})
	mux.HandleFunc("/refill", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Amount   float64 `json:"amount"`
			Resource string  `json:"resource"`
		}
		// A missing or malformed body is a request for the pool's configured
		// refill amount.
		_ = json.NewDecoder(r.Body).Decode(&req)
		resource, err := p.resolveResource(req.Resource)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if req.Amount <= 0 {
			req.Amount = p.cfg.Pools[resource].RefillAmount
		}
		bal, err := p.Refill(r.Context(), resource, req.Amount, "http")
		if errors.Is(err, errSyncedPool) {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, map[string]any{"resource": resource, "balance": bal})
	})
	mux.HandleFunc("/provision", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if p.cfg.Provisioner == nil {
			http.Error(w, "key provisioning is not configured", http.StatusNotImplemented)
			return
		}
		var req struct {
			TaskID string  `json:"task_id"`
			Limit  float64 `json:"limit"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.TaskID == "" || req.Limit <= 0 {
			http.Error(w, "expecting JSON {\"task_id\": \"...\", \"limit\": <positive-number>}", http.StatusBadRequest)
			return
		}
		grant, err := p.cfg.Provisioner.Provision(r.Context(), req.TaskID, req.Limit)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		if err := p.ledger.RecordKeyHash(p.cfg.ProvisionerResource, req.TaskID, grant.Hash); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, map[string]any{
			"task_id":  req.TaskID,
			"resource": p.cfg.ProvisionerResource,
			"key_hash": grant.Hash,
			"key":      grant.Secret,
			"limit":    grant.Limit,
		})
	})
	return mux
}

// formatRequests renders a task's per-pool amounts in name order for log lines.
func formatRequests(reqs map[string]float64) string {
	names := make([]string, 0, len(reqs))
	for name := range reqs {
		names = append(names, name)
	}
	sort.Strings(names)
	parts := make([]string, 0, len(names))
	for _, name := range names {
		parts = append(parts, fmt.Sprintf("%s=%.6g", name, reqs[name]))
	}
	return strings.Join(parts, " ")
}

// debitTask records one launched task's reservation in every pool it names.
// All pools are debited in one ledger update. The task is already running
// when TASK_ADDED arrives, so when a pool holds less than the task carries
// (its balance dropped after the offer went out) the shortfall is logged and
// the reservation is still recorded: that pool's balance drops to zero and
// the ledger keeps the shortfall as the pool's deficit.
func (p *Provider) debitTask(ctx context.Context, taskID string, reqs map[string]float64) {
	changed, err := p.ledger.ReserveMany(taskID, reqs)
	if errors.Is(err, ledger.ErrInsufficient) {
		p.logger.Printf("task %s launched beyond available balance (%v); recording its reservation", taskID, err)
		changed, err = p.ledger.DebitMany(taskID, reqs)
	}
	if err != nil {
		p.logger.Printf("debit error for task %s: %v", taskID, err)
		return
	}
	if changed {
		if err := p.Advertise(ctx, fmt.Sprintf("debited %s for task %s", formatRequests(reqs), taskID)); err != nil {
			p.logger.Printf("advertise error after debit %s: %v", taskID, err)
		}
	}
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
		switch ev.Type {
		case "TASK_ADDED":
			if ev.TaskAdded == nil {
				continue
			}
			taskID := ev.TaskAdded.Task.TaskID.Value
			pid := p.ledger.ProviderID()
			reqs := make(map[string]float64)
			for _, res := range ev.TaskAdded.Task.Resources {
				if p.hasPool(res.Name) && res.Type == "SCALAR" &&
					res.ProviderID != nil && res.ProviderID.Value == pid && res.Scalar != nil {
					reqs[res.Name] += res.Scalar.Value
				}
			}
			p.debitTask(ctx, taskID, reqs)

		case "TASK_UPDATED":
			if ev.TaskUpdated == nil || ev.TaskUpdated.Status == nil {
				continue
			}
			taskID := ev.TaskUpdated.Status.TaskID.Value
			state := ev.TaskUpdated.State
			if state == "" {
				state = ev.TaskUpdated.Status.State
			}
			if taskID == "" || !isTerminalTaskState(state) {
				continue
			}
			if p.cfg.Provisioner != nil {
				p.settleProvisioned(ctx, taskID, state)
			}
			p.releaseSynced(ctx, taskID, state)
		}
	}
}

// settleProvisioned settles an ended task in the provisioner's pool with the
// spend the provisioner reports for the task's key, and revokes the key.
func (p *Provider) settleProvisioned(ctx context.Context, taskID, state string) {
	resource := p.cfg.ProvisionerResource
	keyHash, settled, ok := p.ledger.TaskKeyHash(resource, taskID)
	if !ok || settled {
		return
	}
	actual, err := p.cfg.Provisioner.SettleAndRevoke(ctx, taskID, keyHash)
	if err != nil {
		p.logger.Printf("provisioner settle/revoke failed for task %s (%s): %v", taskID, state, err)
		return
	}
	delta, bal, err := p.Settle(ctx, resource, taskID, actual)
	if err != nil {
		p.logger.Printf("ledger settle failed for task %s: %v", taskID, err)
		return
	}
	p.logger.Printf("auto-settled provisioned task %s in %s on %s actual=%.6g delta=%+.6g balance=%.6g", taskID, resource, state, actual, delta, bal)
}

// releaseSynced closes an ended task's open reservation in every synced pool
// and advertises once if any balance changed. The pool's external reading
// already counts what the task spent, or will at the next read, so the
// reservation is returned whole and no spend figure is needed. Unsynced pools
// are left alone: their reservations stay open until /settle reports the
// spend. The provisioner's pool is settled by settleProvisioned.
func (p *Provider) releaseSynced(ctx context.Context, taskID, state string) {
	var released []string
	for _, name := range p.resources {
		if p.cfg.Pools[name].Source == nil {
			continue
		}
		if p.cfg.Provisioner != nil && name == p.cfg.ProvisionerResource {
			continue
		}
		delta, bal, ok, err := p.ledger.ReleaseTask(name, taskID)
		if err != nil {
			p.logger.Printf("release of task %s in %s failed: %v", taskID, name, err)
			continue
		}
		if !ok {
			continue
		}
		p.logger.Printf("released reservation of task %s in %s on %s amount=%.6g balance=%.6g", taskID, name, state, delta, bal)
		if delta != 0 {
			released = append(released, name)
		}
	}
	if len(released) == 0 {
		return
	}
	reason := fmt.Sprintf("released task %s in %s", taskID, strings.Join(released, ","))
	if err := p.Advertise(ctx, reason); err != nil && ctx.Err() == nil {
		p.logger.Printf("advertise error after release of task %s: %v", taskID, err)
	}
}

// Run subscribes to the node Resource Provider API and controller event stream
// until ctx is cancelled or the provider stream terminates.
func (p *Provider) Run(ctx context.Context) error {
	// Read each synced pool's external balance before subscribing, so the
	// first UPDATE_STATE carries it. A failed read leaves the checkpointed or
	// configured balance in place.
	for _, name := range p.resources {
		if src := p.cfg.Pools[name].Source; src != nil {
			p.syncPool(ctx, name, src)
		}
	}

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
			p.logger.Printf("subscribed provider_id=%s %s", pid, p.formatBalances(p.ledger.Balances()))
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
				if p.cfg.RefillInterval > 0 && p.hasRefillAmount() {
					go func() {
						ticker := time.NewTicker(p.cfg.RefillInterval)
						defer ticker.Stop()
						for {
							select {
							case <-subCtx.Done():
								return
							case <-ticker.C:
								if err := p.RefillAll(subCtx, "timer"); err != nil && subCtx.Err() == nil {
									p.logger.Printf("timer refill failed: %v", err)
								}
							}
						}
					}()
				}
				for _, name := range p.resources {
					opts := p.cfg.Pools[name]
					if opts.Source == nil {
						continue
					}
					go func() {
						ticker := time.NewTicker(opts.SyncInterval)
						defer ticker.Stop()
						for {
							select {
							case <-subCtx.Done():
								return
							case <-ticker.C:
								p.syncPool(subCtx, name, opts.Source)
							}
						}
					}()
				}
			}

		case "PUBLISH_RESOURCES":
			if ev.PublishResources == nil {
				continue
			}
			// The event carries no task ID, so nothing is debited here; the
			// debit happens on TASK_ADDED. The publish is refused when a
			// pool it names is frozen.
			var names []string
			for _, res := range ev.PublishResources.Resources {
				if p.hasPool(res.Name) {
					names = append(names, res.Name)
				}
			}
			status := "OK"
			if !p.ledger.CanAuthorize(names) {
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
