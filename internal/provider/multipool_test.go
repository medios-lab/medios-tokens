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
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/medios-lab/medios-tokens/internal/balancesync"
	"github.com/medios-lab/medios-tokens/internal/ledger"
)

const (
	tau = "tauopt_suggestions"
	usd = "openrouter_usd"
)

// syncBuffer is a log sink that is safe to read while the provider writes.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// harness runs a Provider against a fake node and controller. Every
// UPDATE_STATE is recorded as a map of resource name to advertised value.
type harness struct {
	t      *testing.T
	p      *Provider
	admin  *httptest.Server
	events chan map[string]any

	// drop ends the controller stream that is currently open.
	drop chan struct{}

	mu         sync.Mutex
	advertised []map[string]float64
	connects   int            // controller subscriptions accepted
	snapshot   map[string]any // SUBSCRIBED event sent at the start of each stream, if set
}

func startHarness(t *testing.T, l *ledger.Ledger, cfg Config, logOut io.Writer) *harness {
	t.Helper()
	h := &harness{t: t, events: make(chan map[string]any, 8), drop: make(chan struct{})}
	done := make(chan struct{})
	ctrlReady := make(chan struct{})

	nodeSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var call map[string]any
		_ = json.NewDecoder(r.Body).Decode(&call)
		switch call["type"] {
		case "SUBSCRIBE":
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Medios-Stream-Id", "stream-mp")
			writeRecord(w, w.(http.Flusher), map[string]any{
				"type":       "SUBSCRIBED",
				"subscribed": map[string]any{"provider_id": map[string]any{"value": "rp-test"}},
			})
			<-done
		case "UPDATE_STATE":
			state := map[string]float64{}
			for _, res := range call["update_state"].(map[string]any)["resources"].([]any) {
				m := res.(map[string]any)
				state[m["name"].(string)] = m["scalar"].(map[string]any)["value"].(float64)
			}
			h.mu.Lock()
			h.advertised = append(h.advertised, state)
			h.mu.Unlock()
			w.WriteHeader(http.StatusAccepted)
		}
	}))
	ctrlSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		flusher := w.(http.Flusher)
		flusher.Flush()
		h.mu.Lock()
		h.connects++
		first := h.connects == 1
		snapshot := h.snapshot
		h.mu.Unlock()
		if first {
			close(ctrlReady)
		}
		if snapshot != nil {
			writeRecord(w, flusher, snapshot)
		}
		for {
			select {
			case ev := <-h.events:
				writeRecord(w, flusher, ev)
			case <-h.drop:
				return
			case <-done:
				return
			}
		}
	}))

	cfg.ControllerURL = ctrlSrv.URL
	cfg.NodeURL = nodeSrv.URL
	p, err := New(cfg, l, log.New(logOut, "", 0))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// Tests do not wait out the production resubscribe delays.
	p.ctrlBackoffInitial = 10 * time.Millisecond
	p.ctrlBackoffMax = 40 * time.Millisecond
	h.p = p
	h.admin = httptest.NewServer(p.AdminHandler())

	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = p.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		close(done)
		h.admin.Close()
		ctrlSrv.Close()
		nodeSrv.Close()
	})

	select {
	case <-ctrlReady:
	case <-time.After(3 * time.Second):
		t.Fatal("controller subscriber did not connect")
	}
	return h
}

func (h *harness) states() []map[string]float64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]map[string]float64(nil), h.advertised...)
}

// waitStates blocks until n UPDATE_STATE calls have arrived.
func (h *harness) waitStates(n int) []map[string]float64 {
	h.t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if got := h.states(); len(got) >= n {
			return got
		}
		time.Sleep(10 * time.Millisecond)
	}
	h.t.Fatalf("saw %d UPDATE_STATE calls, want %d: %v", len(h.states()), n, h.states())
	return nil
}

func (h *harness) post(path, body string) (int, map[string]any) {
	h.t.Helper()
	resp, err := http.Post(h.admin.URL+path, "application/json", strings.NewReader(body))
	if err != nil {
		h.t.Fatalf("POST %s: %v", path, err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func (h *harness) getBalance() map[string]any {
	h.t.Helper()
	resp, err := http.Get(h.admin.URL + "/balance")
	if err != nil {
		h.t.Fatalf("GET /balance: %v", err)
	}
	defer resp.Body.Close()
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		h.t.Fatalf("decoding /balance: %v", err)
	}
	return out
}

func scalar(name string, value float64) map[string]any {
	return map[string]any{
		"name":        name,
		"type":        "SCALAR",
		"provider_id": map[string]any{"value": "rp-test"},
		"scalar":      map[string]any{"value": value},
	}
}

func taskAdded(taskID string, resources ...map[string]any) map[string]any {
	return map[string]any{
		"type": "TASK_ADDED",
		"task_added": map[string]any{
			"task": map[string]any{
				"task_id":   map[string]any{"value": taskID},
				"resources": resources,
			},
		},
	}
}

func twoPoolLedger(t *testing.T) *ledger.Ledger {
	t.Helper()
	l, err := ledger.OpenPools("", []ledger.PoolSpec{
		{Resource: tau, Initial: 1000, Cap: 1000},
		{Resource: usd, Initial: 50, Cap: 50},
	})
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func TestMultiPoolAdvertiseDebitSettleRefill(t *testing.T) {
	logs := &syncBuffer{}
	h := startHarness(t, twoPoolLedger(t), Config{}, logs)

	// One UPDATE_STATE carries both pools.
	h.waitStates(1)

	// A task carrying both scalars is debited from both in one step. The cpus
	// entry has no provider ID and is not a pool.
	h.events <- taskAdded("task-1", scalar(tau, 60), scalar(usd, 2),
		map[string]any{"name": "cpus", "type": "SCALAR", "scalar": map[string]any{"value": 1.0}})
	h.waitStates(2)

	// A replayed TASK_ADDED does not debit again; the next event proves it
	// was processed.
	h.events <- taskAdded("task-1", scalar(tau, 60), scalar(usd, 2))

	// A task that carries more than a pool holds is already running, so its
	// reservation is recorded and the short pool drops to zero.
	h.events <- taskAdded("task-2", scalar(tau, 10), scalar(usd, 75))
	h.waitStates(3)

	// With two pools, /settle and /refill need "resource".
	if code, _ := h.post("/settle", `{"task_id":"task-1","actual":45}`); code != http.StatusBadRequest {
		t.Fatalf("/settle without resource = HTTP %d, want 400", code)
	}
	if code, _ := h.post("/refill", `{"amount":1}`); code != http.StatusBadRequest {
		t.Fatalf("/refill without resource = HTTP %d, want 400", code)
	}
	if code, _ := h.post("/settle", `{"task_id":"task-1","actual":45,"resource":"nope"}`); code != http.StatusBadRequest {
		t.Fatalf("/settle with unknown resource = HTTP %d, want 400", code)
	}

	code, body := h.post("/settle", fmt.Sprintf(`{"task_id":"task-1","actual":45,"resource":%q}`, tau))
	if code != http.StatusOK || body["delta"] != 15.0 || body["balance"] != 945.0 || body["resource"] != tau {
		t.Fatalf("/settle = HTTP %d %v, want delta 15 balance 945", code, body)
	}
	code, body = h.post("/refill", fmt.Sprintf(`{"amount":1.5,"resource":%q}`, usd))
	if code != http.StatusOK || body["balance"] != 1.5 || body["resource"] != usd {
		t.Fatalf("/refill = HTTP %d %v, want balance 1.5", code, body)
	}

	want := []map[string]float64{
		{tau: 1000, usd: 50},
		{tau: 940, usd: 48},
		{tau: 930}, // usd is at zero and is not advertised
		{tau: 945},
		{tau: 945, usd: 1.5},
	}
	if got := h.waitStates(len(want)); !reflect.DeepEqual(got, want) {
		t.Fatalf("advertised = %v\nwant        %v", got, want)
	}
	if !strings.Contains(logs.String(), "task task-2 launched beyond available balance") {
		t.Fatalf("over-commit was not logged:\n%s", logs.String())
	}

	// /balance lists every pool and, with two pools, has no single-pool fields.
	bal := h.getBalance()
	pools, _ := bal["pools"].(map[string]any)
	if len(pools) != 2 || pools[tau].(map[string]any)["balance"] != 945.0 || pools[usd].(map[string]any)["balance"] != 1.5 {
		t.Fatalf("/balance pools = %v", bal["pools"])
	}
	if reserved := pools[usd].(map[string]any)["tasks"].(map[string]any)["task-2"].(map[string]any)["reserved"]; reserved != 75.0 {
		t.Fatalf("task-2 reservation in %s = %v, want 75", usd, reserved)
	}
	for _, key := range []string{"balance", "resource", "cap", "tasks"} {
		if _, ok := bal[key]; ok {
			t.Fatalf("/balance with two pools has top-level %q: %v", key, bal)
		}
	}
}

func TestSinglePoolAdminKeepsTopLevelFields(t *testing.T) {
	l, err := ledger.Open("", "token_budget", 100, 100)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := l.ReserveMany("task-1", map[string]float64{"token_budget": 25}); err != nil {
		t.Fatal(err)
	}
	p, err := New(Config{Pools: map[string]PoolOptions{"token_budget": {RefillAmount: 10}}}, l, log.New(io.Discard, "", 0))
	if err != nil {
		t.Fatal(err)
	}
	h := &harness{t: t, p: p, admin: httptest.NewServer(p.AdminHandler())}
	defer h.admin.Close()

	bal := h.getBalance()
	if bal["resource"] != "token_budget" || bal["balance"] != 75.0 || bal["cap"] != 100.0 {
		t.Fatalf("/balance single-pool fields = %v", bal)
	}
	if reserved := bal["tasks"].(map[string]any)["task-1"].(map[string]any)["reserved"]; reserved != 25.0 {
		t.Fatalf("/balance tasks = %v", bal["tasks"])
	}
	if pools := bal["pools"].(map[string]any); pools["token_budget"].(map[string]any)["balance"] != 75.0 {
		t.Fatalf("/balance pools = %v", pools)
	}

	// "resource" is optional with one pool, and accepted when given.
	if code, body := h.post("/settle", `{"task_id":"task-1","actual":18}`); code != http.StatusOK || body["balance"] != 82.0 {
		t.Fatalf("/settle = HTTP %d %v, want balance 82", code, body)
	}
	if code, body := h.post("/refill", `{"amount":5,"resource":"token_budget"}`); code != http.StatusOK || body["balance"] != 87.0 {
		t.Fatalf("/refill with resource = HTTP %d %v, want balance 87", code, body)
	}
	// An empty body refills by the pool's configured amount.
	if code, body := h.post("/refill", ``); code != http.StatusOK || body["balance"] != 97.0 {
		t.Fatalf("/refill with empty body = HTTP %d %v, want balance 97", code, body)
	}
}

func TestNewRejectsInconsistentConfig(t *testing.T) {
	l := twoPoolLedger(t)
	quiet := log.New(io.Discard, "", 0)
	if _, err := New(Config{Pools: map[string]PoolOptions{"nope": {}}}, l, quiet); err == nil {
		t.Fatal("options for a pool outside the ledger were accepted")
	}
	if _, err := New(Config{Provisioner: &fakeProvisioner{}}, l, quiet); err == nil {
		t.Fatal("a provisioner without a pool was accepted with two pools")
	}
	if _, err := New(Config{Provisioner: &fakeProvisioner{}, ProvisionerResource: usd}, l, quiet); err != nil {
		t.Fatalf("provisioner bound to %s: %v", usd, err)
	}
}

func TestProvisionRecordsKeyHashInBoundPool(t *testing.T) {
	l := twoPoolLedger(t)
	p, err := New(Config{Provisioner: &fakeProvisioner{}, ProvisionerResource: usd}, l, log.New(io.Discard, "", 0))
	if err != nil {
		t.Fatal(err)
	}
	h := &harness{t: t, p: p, admin: httptest.NewServer(p.AdminHandler())}
	defer h.admin.Close()

	code, body := h.post("/provision", `{"task_id":"task-k","limit":2}`)
	if code != http.StatusOK || body["resource"] != usd || body["key_hash"] != "hash-task-k" {
		t.Fatalf("/provision = HTTP %d %v", code, body)
	}
	if hash, settled, ok := l.TaskKeyHash(usd, "task-k"); !ok || settled || hash != "hash-task-k" {
		t.Fatalf("TaskKeyHash(%s) = (%q, %v, %v)", usd, hash, settled, ok)
	}
	if _, _, ok := l.TaskKeyHash(tau, "task-k"); ok {
		t.Fatalf("key hash was recorded in %s, which has no provisioner", tau)
	}
}

// quotaServer serves a document shaped like a quota endpoint's response and
// records what it was asked.
type quotaServer struct {
	*httptest.Server
	mu       sync.Mutex
	cap      float64
	used     float64
	status   int
	body     string // overrides the quota document when set
	requests int
	lastAuth string
	onFetch  func(n int) // runs inside the handler before the response
}

const quotaToken = "tok-3f9a-do-not-log"

func newQuotaServer() *quotaServer {
	q := &quotaServer{status: http.StatusOK, cap: 1000}
	q.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q.mu.Lock()
		q.requests++
		n := q.requests
		q.lastAuth = r.Header.Get("Authorization")
		hook := q.onFetch
		q.mu.Unlock()
		if hook != nil {
			hook(n)
		}
		q.mu.Lock()
		defer q.mu.Unlock()
		if q.status != http.StatusOK {
			http.Error(w, "upstream said: "+r.Header.Get("Authorization"), q.status)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if q.body != "" {
			_, _ = io.WriteString(w, q.body)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"org": map[string]any{
				"caps":  map[string]any{"org": map[string]any{"suggests_per_day": q.cap}},
				"usage": map[string]any{"suggests_24h": q.used},
			},
		})
	}))
	return q
}

func (q *quotaServer) set(fn func(q *quotaServer)) {
	q.mu.Lock()
	defer q.mu.Unlock()
	fn(q)
}

// seen returns the request count and the last Authorization header.
func (q *quotaServer) seen() (int, string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.requests, q.lastAuth
}

func (q *quotaServer) poller(t *testing.T) *balancesync.Poller {
	t.Helper()
	poller, err := balancesync.New(balancesync.Config{
		URL:        q.URL + "/v1/me?api_key=" + quotaToken,
		AuthHeader: "Authorization",
		AuthValue:  "Bearer " + quotaToken,
		Timeout:    2 * time.Second,
		CapPath:    "org.caps.org.suggests_per_day",
		UsedPath:   "org.usage.suggests_24h",
	})
	if err != nil {
		t.Fatal(err)
	}
	return poller
}

func TestSyncPoolReconcilesWithUnsettledReservations(t *testing.T) {
	q := newQuotaServer()
	defer q.Close()
	q.set(func(q *quotaServer) { q.used = 987 })

	l := twoPoolLedger(t)
	logs := &syncBuffer{}
	src := q.poller(t)
	p, err := New(Config{Pools: map[string]PoolOptions{tau: {Source: src, SyncInterval: time.Minute}}}, l, log.New(logs, "", 0))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	balance := func() float64 {
		t.Helper()
		bal, err := l.Balance(tau)
		if err != nil {
			t.Fatal(err)
		}
		return bal
	}

	// task-a (5) is unsettled; task-b (3) is settled and no longer in flight.
	for id, amount := range map[string]float64{"task-a": 5, "task-b": 3, "task-c": 1, "task-d": 1, "task-e": 1} {
		if _, err := l.ReserveMany(id, map[string]float64{tau: amount, usd: 1}); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := l.SettleTask(tau, "task-b", 3); err != nil {
		t.Fatal(err)
	}

	// External: 1000 - 987 = 13 available. Unsettled: 5 + 1 + 1 + 1 = 8.
	p.syncPool(ctx, tau, src)
	if got := balance(); got != 5 {
		t.Fatalf("balance after sync = %v, want 5 (13 available - 8 unsettled)", got)
	}
	if _, auth := q.seen(); auth != "Bearer "+quotaToken {
		t.Fatalf("endpoint saw Authorization %q", auth)
	}
	if bal, _ := l.Balance(usd); bal != 45 {
		t.Fatalf("unsynced pool balance = %v, want 45", bal)
	}

	// A failing endpoint keeps the last balance.
	q.set(func(q *quotaServer) { q.status = http.StatusInternalServerError })
	p.syncPool(ctx, tau, src)
	if got := balance(); got != 5 {
		t.Fatalf("balance after HTTP 500 = %v, want 5", got)
	}
	if !strings.Contains(logs.String(), "keeping balance 5") || !strings.Contains(logs.String(), "HTTP 500") {
		t.Fatalf("failed sync was not logged:\n%s", logs.String())
	}

	// So does a response whose document lacks the configured path.
	q.set(func(q *quotaServer) { q.status = http.StatusOK; q.body = `{"org":{"caps":{}}}` })
	p.syncPool(ctx, tau, src)
	if got := balance(); got != 5 {
		t.Fatalf("balance after malformed document = %v, want 5", got)
	}
	if !strings.Contains(logs.String(), `key "org" is missing`) {
		t.Fatalf("missing path was not logged:\n%s", logs.String())
	}

	// And an unreachable endpoint.
	dead := newQuotaServer()
	deadSrc := dead.poller(t)
	dead.Close()
	p.syncPool(ctx, tau, deadSrc)
	if got := balance(); got != 5 {
		t.Fatalf("balance after connection failure = %v, want 5", got)
	}

	// The credential never reaches the log, from the header or the URL query.
	if strings.Contains(logs.String(), quotaToken) {
		t.Fatalf("log contains the credential:\n%s", logs.String())
	}

	// task-a settles (5 spent) while a read is in flight. That reading still
	// reports 987 used; applying it would advertise 13 - 3 = 10 although the
	// endpoint will report 992 used. The reading is discarded and re-read.
	q.set(func(q *quotaServer) {
		q.body = ""
		q.requests = 0
		q.onFetch = func(n int) {
			if n == 1 {
				if _, _, err := l.SettleTask(tau, "task-a", 5); err != nil {
					t.Error(err)
				}
				q.set(func(q *quotaServer) { q.used = 992 })
			}
		}
	})
	p.syncPool(ctx, tau, src)
	if got := balance(); got != 5 {
		t.Fatalf("balance after settle during read = %v, want 5 (8 available - 3 unsettled)", got)
	}
	if n, _ := q.seen(); n != 2 {
		t.Fatalf("endpoint requests = %d, want 2 (one discarded reading, one re-read)", n)
	}

	// When every read overlaps a settlement, the sync stops after
	// maxSyncAttempts reads and leaves the balance to the next interval.
	q.set(func(q *quotaServer) {
		q.requests = 0
		q.used = 0
		q.onFetch = func(n int) {
			id := map[int]string{1: "task-c", 2: "task-d", 3: "task-e"}[n]
			if _, _, err := l.SettleTask(tau, id, 1); err != nil {
				t.Error(err)
			}
		}
	})
	p.syncPool(ctx, tau, src)
	if n, _ := q.seen(); n != maxSyncAttempts {
		t.Fatalf("endpoint requests = %d, want %d", n, maxSyncAttempts)
	}
	if got := balance(); got != 5 {
		t.Fatalf("balance after discarded readings = %v, want 5", got)
	}
	if !strings.Contains(logs.String(), "discarded 2 readings") {
		t.Fatalf("discarded readings were not logged:\n%s", logs.String())
	}

	// A synced pool refuses refills.
	h := &harness{t: t, p: p, admin: httptest.NewServer(p.AdminHandler())}
	defer h.admin.Close()
	if code, _ := h.post("/refill", fmt.Sprintf(`{"amount":10,"resource":%q}`, tau)); code != http.StatusConflict {
		t.Fatalf("/refill on synced pool = HTTP %d, want 409", code)
	}
}

func TestRunAdvertisesSyncedBalance(t *testing.T) {
	q := newQuotaServer()
	defer q.Close()
	q.set(func(q *quotaServer) { q.used = 987 })

	l := twoPoolLedger(t)
	h := startHarness(t, l, Config{
		Pools: map[string]PoolOptions{tau: {Source: q.poller(t), SyncInterval: 20 * time.Millisecond}},
	}, io.Discard)

	// The pool is read before subscribing, so the first UPDATE_STATE carries
	// the external balance and not the configured 1000.
	if got := h.waitStates(1)[0]; !reflect.DeepEqual(got, map[string]float64{tau: 13, usd: 50}) {
		t.Fatalf("first UPDATE_STATE = %v, want %s=13 %s=50", got, tau, usd)
	}

	// A later change at the endpoint is picked up by the interval loop.
	q.set(func(q *quotaServer) { q.used = 990 })
	if got := h.waitStates(2)[1]; !reflect.DeepEqual(got, map[string]float64{tau: 10, usd: 50}) {
		t.Fatalf("second UPDATE_STATE = %v, want %s=10 %s=50", got, tau, usd)
	}

	// A reservation is subtracted from the synced balance.
	h.events <- taskAdded("task-1", scalar(tau, 4))
	if got := h.waitStates(3)[2]; !reflect.DeepEqual(got, map[string]float64{tau: 6, usd: 50}) {
		t.Fatalf("third UPDATE_STATE = %v, want %s=6 %s=50", got, tau, usd)
	}
}

// /balance must be readable while debits and syncs run; run with -race.
func TestBalanceEndpointDuringConcurrentUpdates(t *testing.T) {
	l := twoPoolLedger(t)
	p, err := New(Config{}, l, log.New(io.Discard, "", 0))
	if err != nil {
		t.Fatal(err)
	}
	h := &harness{t: t, p: p, admin: httptest.NewServer(p.AdminHandler())}
	defer h.admin.Close()

	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			p.debitTask(context.Background(), fmt.Sprintf("task-%d", i), map[string]float64{tau: 1, usd: 0.1})
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			_, _ = l.Reconcile(tau, l.SettleEpoch(tau), ledger.External{Available: float64(500 + i), Cap: 1000, HasCap: true})
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			h.getBalance()
		}
	}()
	wg.Wait()
	if bal, _ := l.Balance(usd); bal != 40 {
		t.Fatalf("%s balance = %v, want 40 after 100 debits of 0.1", usd, bal)
	}
}

func TestSyncPoolRetriesFailedAdvertise(t *testing.T) {
	q := newQuotaServer()
	defer q.Close()
	q.set(func(q *quotaServer) { q.used = 1000 })

	var mu sync.Mutex
	failNext := true
	var advertised []map[string]float64
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var call map[string]any
		_ = json.NewDecoder(r.Body).Decode(&call)
		if call["type"] != "UPDATE_STATE" {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		if failNext {
			failNext = false
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		state := map[string]float64{}
		for _, res := range call["update_state"].(map[string]any)["resources"].([]any) {
			m := res.(map[string]any)
			state[m["name"].(string)] = m["scalar"].(map[string]any)["value"].(float64)
		}
		advertised = append(advertised, state)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer node.Close()
	accepted := func() []map[string]float64 {
		mu.Lock()
		defer mu.Unlock()
		return append([]map[string]float64(nil), advertised...)
	}

	l := twoPoolLedger(t)
	if err := l.SetProviderID("rp-test"); err != nil {
		t.Fatal(err)
	}
	logs := &syncBuffer{}
	src := q.poller(t)
	p, err := New(Config{
		NodeURL: node.URL,
		Pools:   map[string]PoolOptions{tau: {Source: src, SyncInterval: time.Minute}},
	}, l, log.New(logs, "", 0))
	if err != nil {
		t.Fatal(err)
	}
	p.mu.Lock()
	p.streamID = "stream-retry"
	p.mu.Unlock()
	ctx := context.Background()

	// The reading moves the pool from 1000 to 0 and the node rejects the
	// UPDATE_STATE that carries it.
	p.syncPool(ctx, tau, src)
	if bal, _ := l.Balance(tau); bal != 0 {
		t.Fatalf("balance after sync = %v, want 0", bal)
	}
	if got := accepted(); len(got) != 0 {
		t.Fatalf("node accepted %v, want nothing yet", got)
	}
	if !strings.Contains(logs.String(), "advertise error after sync") {
		t.Fatalf("log does not record the failed advertisement: %q", logs.String())
	}

	// The same reading changes nothing in the ledger; the advertisement is
	// sent again because the node never accepted the last one.
	p.syncPool(ctx, tau, src)
	want := []map[string]float64{{usd: 50}}
	if got := accepted(); !reflect.DeepEqual(got, want) {
		t.Fatalf("node accepted %v, want %v", got, want)
	}

	// Once accepted, an unchanged reading sends nothing more.
	p.syncPool(ctx, tau, src)
	if got := accepted(); len(got) != 1 {
		t.Fatalf("node accepted %d states after an unchanged reading, want 1", len(got))
	}
}

func taskUpdated(taskID, state string) map[string]any {
	return map[string]any{
		"type": "TASK_UPDATED",
		"task_updated": map[string]any{
			"state": state,
			"status": map[string]any{
				"task_id": map[string]any{"value": taskID},
				"state":   state,
			},
		},
	}
}

// syncedHarness starts a provider with a synced pool (tau: cap 100, 38
// available externally) and an unsynced one (usd: 50). The sync interval is
// long enough that only the read before SUBSCRIBE happens during a test, so
// every later balance change comes from task events and admin calls.
func syncedHarness(t *testing.T, statePath string) (*harness, *ledger.Ledger, *quotaServer) {
	t.Helper()
	q := newQuotaServer()
	t.Cleanup(q.Close)
	q.set(func(q *quotaServer) { q.cap, q.used = 100, 62 })
	l, err := ledger.OpenPools(statePath, []ledger.PoolSpec{
		{Resource: tau, Initial: 100, Cap: 100},
		{Resource: usd, Initial: 50, Cap: 50},
	})
	if err != nil {
		t.Fatal(err)
	}
	h := startHarness(t, l, Config{
		Pools: map[string]PoolOptions{tau: {Source: q.poller(t), SyncInterval: time.Minute}},
	}, io.Discard)
	return h, l, q
}

func wantStates(t *testing.T, h *harness, want ...map[string]float64) {
	t.Helper()
	if got := h.waitStates(len(want)); !reflect.DeepEqual(got, want) {
		t.Fatalf("UPDATE_STATE sequence = %v, want %v", got, want)
	}
}

// waitClosed blocks until the task's entry in the synced pool is settled.
func waitClosed(t *testing.T, l *ledger.Ledger, taskID string) ledger.TaskEntry {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if e := l.Snapshot().Pools[tau].Tasks[taskID]; e.Settled {
			return e
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("reservation of %s in %s was not closed", taskID, tau)
	return ledger.TaskEntry{}
}

func TestSyncedPoolClosesReservationWhenTaskEnds(t *testing.T) {
	for _, state := range []string{"TASK_FINISHED", "TASK_KILLED", "TASK_LOST"} {
		t.Run(state, func(t *testing.T) {
			h, l, q := syncedHarness(t, "")
			h.events <- taskAdded("task-1", scalar(tau, 20), scalar(usd, 2))
			wantStates(t, h,
				map[string]float64{tau: 38, usd: 50},
				map[string]float64{tau: 18, usd: 48},
			)

			// A non-terminal update leaves the reservation open. The debit
			// behind it on the stream proves it was processed.
			h.events <- taskUpdated("task-1", "TASK_RUNNING")
			h.events <- taskAdded("task-2", scalar(usd, 1))
			wantStates(t, h,
				map[string]float64{tau: 38, usd: 50},
				map[string]float64{tau: 18, usd: 48},
				map[string]float64{tau: 18, usd: 47},
			)
			if e := l.Snapshot().Pools[tau].Tasks["task-1"]; e.Settled {
				t.Fatalf("TASK_RUNNING closed the reservation: %+v", e)
			}

			// The terminal update closes the reservation and leaves the
			// balance at 18.
			h.events <- taskUpdated("task-1", state)
			if e := waitClosed(t, l, "task-1"); !e.Released || e.Actual != 20 {
				t.Fatalf("synced pool entry = %+v, want released with actual 20", e)
			}
			if bal, _ := l.Balance(tau); bal != 18 {
				t.Fatalf("balance after %s = %v, want 18", state, bal)
			}
			// The unsynced pool's reservation stays open until /settle.
			if e := l.Snapshot().Pools[usd].Tasks["task-1"]; e.Settled || e.Reserved != 2 {
				t.Fatalf("unsynced pool entry = %+v, want unsettled reservation of 2", e)
			}
			if n, _ := q.seen(); n != 1 {
				t.Fatalf("endpoint was read %d times, want 1", n)
			}

			// The next read reports 30 remaining and that is advertised
			// whole: the closed reservation is not subtracted from it.
			q.set(func(q *quotaServer) { q.used = 70 })
			h.p.syncPool(context.Background(), tau, q.poller(t))
			wantStates(t, h,
				map[string]float64{tau: 38, usd: 50},
				map[string]float64{tau: 18, usd: 48},
				map[string]float64{tau: 18, usd: 47},
				map[string]float64{tau: 30, usd: 47},
			)
		})
	}
}

func TestSettleAfterAutomaticCloseChangesNothing(t *testing.T) {
	h, l, _ := syncedHarness(t, "")
	h.events <- taskAdded("task-1", scalar(tau, 20))
	h.events <- taskUpdated("task-1", "TASK_FINISHED")
	waitClosed(t, l, "task-1")

	code, body := h.post("/settle", fmt.Sprintf(`{"task_id":"task-1","actual":5,"resource":%q}`, tau))
	if code != http.StatusOK || body["delta"] != 0.0 || body["balance"] != 18.0 {
		t.Fatalf("/settle after the terminal update = HTTP %d %v, want 200 delta 0 balance 18", code, body)
	}
	wantStates(t, h,
		map[string]float64{tau: 38, usd: 50},
		map[string]float64{tau: 18, usd: 50},
	)
	if e := l.Snapshot().Pools[tau].Tasks["task-1"]; !e.Released || e.Actual != 20 {
		t.Fatalf("entry = %+v, want the automatic close kept", e)
	}
}

func TestTerminalUpdateAfterSettleChangesNothing(t *testing.T) {
	h, l, _ := syncedHarness(t, "")
	h.events <- taskAdded("task-1", scalar(tau, 20))
	wantStates(t, h,
		map[string]float64{tau: 38, usd: 50},
		map[string]float64{tau: 18, usd: 50},
	)

	code, body := h.post("/settle", fmt.Sprintf(`{"task_id":"task-1","actual":5,"resource":%q}`, tau))
	if code != http.StatusOK || body["delta"] != 15.0 || body["balance"] != 33.0 {
		t.Fatalf("/settle = HTTP %d %v, want 200 delta 15 balance 33", code, body)
	}

	// The terminal update finds the task settled. The debit that follows it
	// on the stream produces the next UPDATE_STATE, from 33.
	h.events <- taskUpdated("task-1", "TASK_FINISHED")
	h.events <- taskAdded("task-2", scalar(tau, 1))
	wantStates(t, h,
		map[string]float64{tau: 38, usd: 50},
		map[string]float64{tau: 18, usd: 50},
		map[string]float64{tau: 33, usd: 50},
		map[string]float64{tau: 32, usd: 50},
	)
	if e := l.Snapshot().Pools[tau].Tasks["task-1"]; e.Released || e.Actual != 5 {
		t.Fatalf("entry = %+v, want the explicit settlement kept", e)
	}
}

func TestReservationIsClosedAfterRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")

	t.Run("reserve", func(t *testing.T) {
		h, _, _ := syncedHarness(t, path)
		h.events <- taskAdded("task-1", scalar(tau, 20))
		wantStates(t, h,
			map[string]float64{tau: 38, usd: 50},
			map[string]float64{tau: 18, usd: 50},
		)
	})

	// A new process loads the checkpoint. Its first read subtracts the
	// reservation the checkpoint kept, and the terminal update closes it.
	h, l, q := syncedHarness(t, path)
	if e := l.Snapshot().Pools[tau].Tasks["task-1"]; e.Settled || e.Reserved != 20 {
		t.Fatalf("entry after restart = %+v, want unsettled reservation of 20", e)
	}
	wantStates(t, h, map[string]float64{tau: 18, usd: 50})
	h.events <- taskUpdated("task-1", "TASK_FINISHED")
	waitClosed(t, l, "task-1")

	// The next read is applied without the closed reservation.
	q.set(func(q *quotaServer) { q.used = 70 })
	h.p.syncPool(context.Background(), tau, q.poller(t))
	wantStates(t, h,
		map[string]float64{tau: 18, usd: 50},
		map[string]float64{tau: 30, usd: 50},
	)
}

// snapshotTask is one Task of a controller snapshot.
func snapshotTask(taskID, state string, resources ...map[string]any) map[string]any {
	return map[string]any{
		"task_id":   map[string]any{"value": taskID},
		"state":     state,
		"resources": resources,
	}
}

func subscribed(tasks, completed []map[string]any) map[string]any {
	return map[string]any{
		"type": "SUBSCRIBED",
		"subscribed": map[string]any{
			"get_state": map[string]any{
				"get_tasks": map[string]any{"tasks": tasks, "completed_tasks": completed},
			},
		},
	}
}

func (h *harness) waitConnects(n int) {
	h.t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		h.mu.Lock()
		got := h.connects
		h.mu.Unlock()
		if got >= n {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	h.t.Fatalf("controller saw fewer than %d subscriptions", n)
}

func TestControllerWatcherResubscribesAndAppliesSnapshot(t *testing.T) {
	q := newQuotaServer()
	defer q.Close()
	q.set(func(q *quotaServer) { q.cap, q.used = 100, 62 })
	l, err := ledger.OpenPools("", []ledger.PoolSpec{
		{Resource: tau, Initial: 100, Cap: 100},
		{Resource: usd, Initial: 50, Cap: 50},
	})
	if err != nil {
		t.Fatal(err)
	}
	logs := &syncBuffer{}
	h := startHarness(t, l, Config{
		Pools: map[string]PoolOptions{tau: {Source: q.poller(t), SyncInterval: time.Minute}},
	}, logs)

	// Three tasks are running when the stream goes down. task-b also holds
	// the unsynced pool.
	h.events <- taskAdded("task-a", scalar(tau, 20))
	h.events <- taskAdded("task-b", scalar(tau, 5), scalar(usd, 2))
	h.events <- taskAdded("task-c", scalar(tau, 3))
	wantStates(t, h,
		map[string]float64{tau: 38, usd: 50},
		map[string]float64{tau: 18, usd: 50},
		map[string]float64{tau: 13, usd: 48},
		map[string]float64{tau: 10, usd: 48},
	)

	// While it is down: task-a finishes, task-b disappears from the
	// controller, task-c keeps running and task-d is launched.
	h.mu.Lock()
	h.snapshot = subscribed(
		[]map[string]any{
			snapshotTask("task-c", "TASK_RUNNING", scalar(tau, 3)),
			snapshotTask("task-d", "TASK_RUNNING", scalar(tau, 2)),
		},
		[]map[string]any{
			snapshotTask("task-a", "TASK_FINISHED", scalar(tau, 20)),
			snapshotTask("task-old", "TASK_FINISHED", scalar(tau, 50)),
		},
	)
	h.mu.Unlock()
	h.drop <- struct{}{}
	h.waitConnects(2)

	// The snapshot debits task-d only: task-c is not debited again and the
	// ended task-old, which the ledger never held, is not debited at all.
	// A TASK_ADDED on the new stream is debited.
	h.events <- taskAdded("task-e", scalar(tau, 1))
	wantStates(t, h,
		map[string]float64{tau: 38, usd: 50},
		map[string]float64{tau: 18, usd: 50},
		map[string]float64{tau: 13, usd: 48},
		map[string]float64{tau: 10, usd: 48},
		map[string]float64{tau: 8, usd: 48},
		map[string]float64{tau: 7, usd: 48},
	)

	// The reservations of the ended and the absent task are settled as
	// spent in the synced pool; the balance was not raised for them.
	for _, id := range []string{"task-a", "task-b"} {
		if e := waitClosed(t, l, id); !e.Released || e.Actual != e.Reserved {
			t.Fatalf("%s entry = %+v, want released with actual = reserved", id, e)
		}
	}
	snap := l.Snapshot()
	for _, id := range []string{"task-c", "task-d", "task-e"} {
		if e := snap.Pools[tau].Tasks[id]; e.Settled || e.Reserved == 0 {
			t.Fatalf("%s entry = %+v, want an open reservation", id, e)
		}
	}
	if _, ok := snap.Pools[tau].Tasks["task-old"]; ok {
		t.Fatal("an ended task the ledger never held was debited from the snapshot")
	}
	// The unsynced pool still waits for /settle.
	if e := snap.Pools[usd].Tasks["task-b"]; e.Settled || e.Reserved != 2 {
		t.Fatalf("unsynced entry of task-b = %+v, want unsettled reservation of 2", e)
	}

	// The next read subtracts only the three open reservations: 30 - 6.
	q.set(func(q *quotaServer) { q.used = 70 })
	h.p.syncPool(context.Background(), tau, q.poller(t))
	if got := h.waitStates(7)[6]; !reflect.DeepEqual(got, map[string]float64{tau: 24, usd: 48}) {
		t.Fatalf("UPDATE_STATE after the next read = %v, want %s=24", got, tau)
	}

	out := logs.String()
	for _, want := range []string{"controller event stream disconnected", "controller event stream connected"} {
		if !strings.Contains(out, want) {
			t.Fatalf("log lacks %q:\n%s", want, out)
		}
	}
}

func TestControllerWatcherRetriesUntilConnected(t *testing.T) {
	var mu sync.Mutex
	attempts := 0
	done := make(chan struct{})
	ctrl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		attempts++
		n := attempts
		mu.Unlock()
		if n <= 2 {
			http.Error(w, "not leading", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		flusher := w.(http.Flusher)
		flusher.Flush()
		writeRecord(w, flusher, taskAdded("task-1", scalar(tau, 4)))
		select {
		case <-done:
		case <-r.Context().Done():
		}
	}))
	defer ctrl.Close()
	defer close(done)

	l := twoPoolLedger(t)
	if err := l.SetProviderID("rp-test"); err != nil {
		t.Fatal(err)
	}
	logs := &syncBuffer{}
	p, err := New(Config{ControllerURL: ctrl.URL}, l, log.New(logs, "", 0))
	if err != nil {
		t.Fatal(err)
	}
	p.ctrlBackoffInitial = 5 * time.Millisecond
	p.ctrlBackoffMax = 20 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() {
		p.runControllerWatcher(ctx)
		close(stopped)
	}()

	deadline := time.Now().Add(3 * time.Second)
	for {
		if bal, _ := l.Balance(tau); bal == 996 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("task on the third subscription was not debited; log:\n%s", logs.String())
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := strings.Count(logs.String(), "controller event stream not connected"); got != 2 {
		t.Fatalf("logged %d failed subscriptions, want 2:\n%s", got, logs.String())
	}

	// Cancelling the context ends the watcher.
	cancel()
	select {
	case <-stopped:
	case <-time.After(3 * time.Second):
		t.Fatal("watcher did not stop after its context was cancelled")
	}
}
