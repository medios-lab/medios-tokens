package ledger

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

const tb = "token_budget"

func mustBalance(t *testing.T, l *Ledger, resource string) float64 {
	t.Helper()
	bal, err := l.Balance(resource)
	if err != nil {
		t.Fatalf("Balance(%s): %v", resource, err)
	}
	return bal
}

// reserve debits one pool and returns (balanceAfter, changed, err).
func reserve(t *testing.T, l *Ledger, resource, taskID string, amount float64) (float64, bool, error) {
	t.Helper()
	changed, err := l.ReserveMany(taskID, map[string]float64{resource: amount})
	return mustBalance(t, l, resource), changed, err
}

func TestLedgerReserveSettleRefillAndCheckpoint(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "ledger.json")

	l, err := Open(stateFile, tb, 100, 100)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := l.SetProviderID("rp-1"); err != nil {
		t.Fatalf("SetProviderID: %v", err)
	}

	// Debit once; duplicate debit is a no-op.
	bal, changed, err := reserve(t, l, tb, "task-1", 25)
	if err != nil || !changed || bal != 75 {
		t.Fatalf("reserve #1 = (%v, %v, %v), want (75, true, nil)", bal, changed, err)
	}
	bal, changed, err = reserve(t, l, tb, "task-1", 25)
	if err != nil || changed || bal != 75 {
		t.Fatalf("reserve duplicate = (%v, %v, %v), want (75, false, nil)", bal, changed, err)
	}

	// Settle task-1 with actual=18 -> releases 7 unused back to 82.
	delta, bal, err := l.SettleTask(tb, "task-1", 18)
	if err != nil || delta != 7 || bal != 82 {
		t.Fatalf("SettleTask = (%v, %v, %v), want (7, 82, nil)", delta, bal, err)
	}
	// Duplicate settle is idempotent.
	delta, bal, err = l.SettleTask(tb, "task-1", 18)
	if err != nil || delta != 0 || bal != 82 {
		t.Fatalf("SettleTask duplicate = (%v, %v, %v), want (0, 82, nil)", delta, bal, err)
	}

	// Refill clamps at cap=100.
	bal, changed, err = l.Refill(tb, 50)
	if err != nil || !changed || bal != 100 {
		t.Fatalf("Refill = (%v, %v, %v), want (100, true, nil)", bal, changed, err)
	}

	// Debit down to 40, then reopen from checkpoint and verify survival.
	if _, _, err := reserve(t, l, tb, "task-2", 60); err != nil {
		t.Fatalf("reserve task-2: %v", err)
	}

	reopened, err := Open(stateFile, tb, 100, 100)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if reopened.ProviderID() != "rp-1" {
		t.Fatalf("reopened ProviderID = %q, want rp-1", reopened.ProviderID())
	}
	if bal := mustBalance(t, reopened, tb); bal != 40 {
		t.Fatalf("reopened Balance = %v, want 40", bal)
	}
	// Previously debited task stays deduplicated after restart.
	if bal, changed, _ := reserve(t, reopened, tb, "task-2", 60); changed || bal != 40 {
		t.Fatalf("post-restart duplicate debit = (%v, %v), want (40, false)", bal, changed)
	}
}

func TestLedgerRecordKeyHashAndCheckpoint(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "ledger.json")

	l, err := Open(stateFile, tb, 100, 100)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	// Pattern 2 provisions a scoped key before TASK_ADDED arrives.
	if err := l.RecordKeyHash(tb, "task-prov", "or-hash-123"); err != nil {
		t.Fatalf("RecordKeyHash: %v", err)
	}
	bal, changed, err := reserve(t, l, tb, "task-prov", 25)
	if err != nil || !changed || bal != 75 {
		t.Fatalf("reserve after RecordKeyHash = (%v, %v, %v), want (75, true, nil)", bal, changed, err)
	}
	if bal, changed, err := reserve(t, l, tb, "task-prov", 25); err != nil || changed || bal != 75 {
		t.Fatalf("duplicate reserve = (%v, %v, %v), want (75, false, nil)", bal, changed, err)
	}

	reopened, err := Open(stateFile, tb, 100, 100)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	hash, settled, ok := reopened.TaskKeyHash(tb, "task-prov")
	if !ok || settled || hash != "or-hash-123" {
		t.Fatalf("TaskKeyHash after restart = (%q, %v, %v), want (or-hash-123, false, true)", hash, settled, ok)
	}

	delta, bal, err := reopened.SettleTask(tb, "task-prov", 10)
	if err != nil || delta != 15 || bal != 90 {
		t.Fatalf("SettleTask = (%v, %v, %v), want (15, 90, nil)", delta, bal, err)
	}
	hash, settled, ok = reopened.TaskKeyHash(tb, "task-prov")
	if !ok || !settled || hash != "or-hash-123" {
		t.Fatalf("TaskKeyHash after settle = (%q, %v, %v), want (or-hash-123, true, true)", hash, settled, ok)
	}
}

var twoPools = []PoolSpec{
	{Resource: "tauopt_suggestions", Initial: 1000, Cap: 1000},
	{Resource: "openrouter_usd", Initial: 50, Cap: 50},
}

func TestMultiPoolCheckpointRoundTrip(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "ledger.json")
	l, err := OpenPools(stateFile, twoPools)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := l.SetProviderID("rp-2"); err != nil {
		t.Fatal(err)
	}
	changed, err := l.ReserveMany("task-1", map[string]float64{"tauopt_suggestions": 60, "openrouter_usd": 2})
	if err != nil || !changed {
		t.Fatalf("ReserveMany = (%v, %v), want (true, nil)", changed, err)
	}
	if _, _, err := l.SettleTask("openrouter_usd", "task-1", 0.5); err != nil {
		t.Fatal(err)
	}

	raw, err := os.ReadFile(stateFile)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"version": 2`) || !strings.Contains(string(raw), `"pools"`) {
		t.Fatalf("checkpoint is not in the multi-pool format: %s", raw)
	}

	reopened, err := OpenPools(stateFile, twoPools)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	snap := reopened.Snapshot()
	if snap.Version != SnapshotVersion || snap.ProviderID != "rp-2" || len(snap.Pools) != 2 {
		t.Fatalf("reopened snapshot = %+v", snap)
	}
	tau, usd := snap.Pools["tauopt_suggestions"], snap.Pools["openrouter_usd"]
	if tau.Balance != 940 || tau.Cap != 1000 || tau.Tasks["task-1"] != (TaskEntry{Reserved: 60}) {
		t.Fatalf("tauopt pool after reopen = %+v", tau)
	}
	if usd.Balance != 49.5 || usd.Cap != 50 || usd.Tasks["task-1"] != (TaskEntry{Reserved: 2, Settled: true, Actual: 0.5}) {
		t.Fatalf("openrouter pool after reopen = %+v", usd)
	}
	// The reopened ledger still deduplicates the task.
	if changed, err := reopened.ReserveMany("task-1", map[string]float64{"tauopt_suggestions": 60}); err != nil || changed {
		t.Fatalf("duplicate ReserveMany after reopen = (%v, %v), want (false, nil)", changed, err)
	}
}

const v1Checkpoint = `{
  "provider_id": "rp-old",
  "resource": "token_budget",
  "balance": 57,
  "cap": 100,
  "tasks": {
    "task-a": {"reserved": 25, "settled": true, "actual": 18},
    "task-b": {"reserved": 25, "settled": false, "key_hash": "h-b"}
  }
}
`

func TestSinglePoolCheckpointMigration(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "ledger.json")
	if err := os.WriteFile(stateFile, []byte(v1Checkpoint), 0o600); err != nil {
		t.Fatal(err)
	}

	// The flags' initial and cap do not override checkpointed state.
	l, err := Open(stateFile, tb, 100, 100)
	if err != nil {
		t.Fatalf("open single-pool checkpoint: %v", err)
	}
	snap := l.Snapshot()
	if snap.Version != SnapshotVersion || snap.ProviderID != "rp-old" || len(snap.Pools) != 1 {
		t.Fatalf("migrated snapshot = %+v", snap)
	}
	pool := snap.Pools[tb]
	if pool == nil || pool.Balance != 57 || pool.Cap != 100 || len(pool.Tasks) != 2 {
		t.Fatalf("migrated pool = %+v", pool)
	}
	if hash, settled, ok := l.TaskKeyHash(tb, "task-b"); !ok || settled || hash != "h-b" {
		t.Fatalf("TaskKeyHash(task-b) = (%q, %v, %v)", hash, settled, ok)
	}
	// Loading does not rewrite the file.
	raw, _ := os.ReadFile(stateFile)
	if string(raw) != v1Checkpoint {
		t.Fatalf("checkpoint was rewritten on load: %s", raw)
	}
	// A task debited before the upgrade stays deduplicated.
	if bal, changed, err := reserve(t, l, tb, "task-b", 25); err != nil || changed || bal != 57 {
		t.Fatalf("duplicate reserve after migration = (%v, %v, %v), want (57, false, nil)", bal, changed, err)
	}
	// The first mutation writes the multi-pool format, which reopens cleanly.
	if _, _, err := l.SettleTask(tb, "task-b", 5); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(stateFile, tb, 100, 100)
	if err != nil {
		t.Fatalf("reopen after migration: %v", err)
	}
	if bal := mustBalance(t, reopened, tb); bal != 77 {
		t.Fatalf("balance after migration and settle = %v, want 77", bal)
	}
}

func TestSinglePoolCheckpointGainsConfiguredPool(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "ledger.json")
	if err := os.WriteFile(stateFile, []byte(v1Checkpoint), 0o600); err != nil {
		t.Fatal(err)
	}
	l, err := OpenPools(stateFile, []PoolSpec{
		{Resource: tb, Initial: 100, Cap: 100},
		{Resource: "tauopt_suggestions", Initial: 1000, Cap: 1000},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if got := l.Resources(); len(got) != 2 || got[0] != "tauopt_suggestions" || got[1] != tb {
		t.Fatalf("Resources = %v", got)
	}
	if bal := mustBalance(t, l, tb); bal != 57 {
		t.Fatalf("migrated balance = %v, want 57", bal)
	}
	if bal := mustBalance(t, l, "tauopt_suggestions"); bal != 1000 {
		t.Fatalf("added pool balance = %v, want 1000", bal)
	}
}

func TestOpenRejectsBadCheckpoints(t *testing.T) {
	cases := map[string]struct {
		file  string
		specs []PoolSpec
		want  string
	}{
		"pool not configured": {
			file:  v1Checkpoint,
			specs: []PoolSpec{{Resource: "other", Initial: 1, Cap: 1}},
			want:  `holds pool "token_budget", which is not configured`,
		},
		"single-pool without resource": {
			file:  `{"balance": 5, "cap": 5, "tasks": {}}`,
			specs: []PoolSpec{{Resource: tb, Initial: 1, Cap: 1}},
			want:  "no resource name",
		},
		"future version": {
			file:  `{"version": 3, "pools": {"token_budget": {"balance": 1, "cap": 1, "tasks": {}}}}`,
			specs: []PoolSpec{{Resource: tb, Initial: 1, Cap: 1}},
			want:  "unsupported checkpoint version 3",
		},
		"null pool": {
			file:  `{"version": 2, "pools": {"token_budget": null}}`,
			specs: []PoolSpec{{Resource: tb, Initial: 1, Cap: 1}},
			want:  "empty or unnamed",
		},
		"not json": {
			file:  `{`,
			specs: []PoolSpec{{Resource: tb, Initial: 1, Cap: 1}},
			want:  "corrupt ledger checkpoint",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			stateFile := filepath.Join(t.TempDir(), "ledger.json")
			if err := os.WriteFile(stateFile, []byte(tc.file), 0o600); err != nil {
				t.Fatal(err)
			}
			_, err := OpenPools(stateFile, tc.specs)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("OpenPools error = %v, want it to contain %q", err, tc.want)
			}
		})
	}
	if _, err := OpenPools("", []PoolSpec{{Resource: "a"}, {Resource: "a"}}); err == nil {
		t.Fatal("duplicate pool spec was accepted")
	}
	if _, err := OpenPools("", nil); err == nil {
		t.Fatal("empty pool list was accepted")
	}
}

func TestReserveManyFailsClosed(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "ledger.json")
	l, err := OpenPools(stateFile, twoPools)
	if err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(stateFile)
	if err != nil {
		t.Fatal(err)
	}

	assertUntouched := func(step string) {
		t.Helper()
		snap := l.Snapshot()
		tau, usd := snap.Pools["tauopt_suggestions"], snap.Pools["openrouter_usd"]
		if tau.Balance != 1000 || usd.Balance != 50 || len(tau.Tasks) != 0 || len(usd.Tasks) != 0 {
			t.Fatalf("%s changed the ledger: tauopt=%+v openrouter=%+v", step, tau, usd)
		}
		after, err := os.ReadFile(stateFile)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(before, after) {
			t.Fatalf("%s changed the checkpoint:\n%s", step, after)
		}
	}

	// tauopt can cover 60; openrouter cannot cover 75.
	changed, err := l.ReserveMany("task-1", map[string]float64{"tauopt_suggestions": 60, "openrouter_usd": 75})
	if !errors.Is(err, ErrInsufficient) || changed {
		t.Fatalf("ReserveMany = (%v, %v), want (false, ErrInsufficient)", changed, err)
	}
	assertUntouched("insufficient ReserveMany")

	changed, err = l.ReserveMany("task-1", map[string]float64{"tauopt_suggestions": 60, "nope": 1})
	if !errors.Is(err, ErrUnknownPool) || changed {
		t.Fatalf("ReserveMany with unknown pool = (%v, %v), want (false, ErrUnknownPool)", changed, err)
	}
	assertUntouched("unknown-pool ReserveMany")

	// The same task can still be admitted once both pools cover it.
	changed, err = l.ReserveMany("task-1", map[string]float64{"tauopt_suggestions": 60, "openrouter_usd": 2})
	if err != nil || !changed {
		t.Fatalf("ReserveMany = (%v, %v), want (true, nil)", changed, err)
	}
	if tau, usd := mustBalance(t, l, "tauopt_suggestions"), mustBalance(t, l, "openrouter_usd"); tau != 940 || usd != 48 {
		t.Fatalf("balances after reserve = (%v, %v), want (940, 48)", tau, usd)
	}
}

func TestDebitManyRecordsShortfall(t *testing.T) {
	l, err := OpenPools("", twoPools)
	if err != nil {
		t.Fatal(err)
	}
	changed, err := l.DebitMany("task-1", map[string]float64{"tauopt_suggestions": 60, "openrouter_usd": 75})
	if err != nil || !changed {
		t.Fatalf("DebitMany = (%v, %v), want (true, nil)", changed, err)
	}
	snap := l.Snapshot()
	if got := snap.Pools["openrouter_usd"]; got.Balance != 0 || got.Tasks["task-1"].Reserved != 75 {
		t.Fatalf("short pool = %+v, want balance 0 and reservation 75", got)
	}
	if got := snap.Pools["tauopt_suggestions"]; got.Balance != 940 || got.Tasks["task-1"].Reserved != 60 {
		t.Fatalf("covered pool = %+v, want balance 940 and reservation 60", got)
	}
}

// A failed checkpoint write leaves the in-memory ledger as it was.
func TestFailedCheckpointWriteChangesNothing(t *testing.T) {
	dir := t.TempDir()
	stateFile := filepath.Join(dir, "ledger.json")
	l, err := OpenPools(stateFile, twoPools)
	if err != nil {
		t.Fatal(err)
	}
	// A directory at the temporary path makes the checkpoint write fail.
	if err := os.Mkdir(stateFile+".tmp", 0o755); err != nil {
		t.Fatal(err)
	}
	changed, err := l.ReserveMany("task-1", map[string]float64{"tauopt_suggestions": 60, "openrouter_usd": 2})
	if err == nil || changed {
		t.Fatalf("ReserveMany = (%v, %v), want a write error", changed, err)
	}
	snap := l.Snapshot()
	if snap.Pools["tauopt_suggestions"].Balance != 1000 || snap.Pools["openrouter_usd"].Balance != 50 ||
		len(snap.Pools["tauopt_suggestions"].Tasks) != 0 {
		t.Fatalf("failed write changed memory: %+v %+v", snap.Pools["tauopt_suggestions"], snap.Pools["openrouter_usd"])
	}
}

func TestReconcileSubtractsUnsettledReservations(t *testing.T) {
	l, err := OpenPools("", []PoolSpec{{Resource: "q", Initial: 1000, Cap: 1000}})
	if err != nil {
		t.Fatal(err)
	}
	for id, amount := range map[string]float64{"a": 60, "b": 40, "c": 30} {
		if _, err := l.ReserveMany(id, map[string]float64{"q": amount}); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := l.SettleTask("q", "c", 30); err != nil {
		t.Fatal(err)
	}

	// External: cap 1000, 987 used -> 13 available; 100 reserved and unsettled.
	changed, err := l.Reconcile("q", l.SettleEpoch("q"), External{Available: 13, Cap: 1000, HasCap: true})
	if err != nil || !changed {
		t.Fatalf("Reconcile = (%v, %v), want (true, nil)", changed, err)
	}
	if bal := mustBalance(t, l, "q"); bal != 0 {
		t.Fatalf("balance = %v, want 0 (13 available - 100 unsettled, floored)", bal)
	}

	// External: cap 2000, 1500 available; settled task c is not subtracted.
	changed, err = l.Reconcile("q", l.SettleEpoch("q"), External{Available: 1500, Cap: 2000, HasCap: true})
	if err != nil || !changed {
		t.Fatalf("Reconcile = (%v, %v), want (true, nil)", changed, err)
	}
	snap := l.Snapshot()
	if got := snap.Pools["q"]; got.Balance != 1400 || got.Cap != 2000 {
		t.Fatalf("pool = %+v, want balance 1400 cap 2000", got)
	}
	// An identical reading changes nothing.
	if changed, err := l.Reconcile("q", l.SettleEpoch("q"), External{Available: 1500, Cap: 2000, HasCap: true}); err != nil || changed {
		t.Fatalf("repeat Reconcile = (%v, %v), want (false, nil)", changed, err)
	}
	// Without an external cap, the cap is kept unless the balance exceeds it.
	if _, err := l.Reconcile("q", l.SettleEpoch("q"), External{Available: 500}); err != nil {
		t.Fatal(err)
	}
	if got := l.Snapshot().Pools["q"]; got.Balance != 400 || got.Cap != 2000 {
		t.Fatalf("pool = %+v, want balance 400 cap 2000", got)
	}
	if _, err := l.Reconcile("q", l.SettleEpoch("q"), External{Available: 2600}); err != nil {
		t.Fatal(err)
	}
	if got := l.Snapshot().Pools["q"]; got.Balance != 2500 || got.Cap != 2600 {
		t.Fatalf("pool = %+v, want balance 2500 cap 2600", got)
	}
}

func TestReconcileRejectsReadingTakenBeforeSettlement(t *testing.T) {
	l, err := OpenPools("", []PoolSpec{{Resource: "q", Initial: 100, Cap: 100}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := l.ReserveMany("a", map[string]float64{"q": 40}); err != nil {
		t.Fatal(err)
	}
	epoch := l.SettleEpoch("q")
	// The task settles with all 40 spent while the external read is in flight.
	// The reading, taken earlier, still shows 100 available.
	if _, _, err := l.SettleTask("q", "a", 40); err != nil {
		t.Fatal(err)
	}
	changed, err := l.Reconcile("q", epoch, External{Available: 100, Cap: 100, HasCap: true})
	if !errors.Is(err, ErrStaleReading) || changed {
		t.Fatalf("Reconcile = (%v, %v), want (false, ErrStaleReading)", changed, err)
	}
	if bal := mustBalance(t, l, "q"); bal != 60 {
		t.Fatalf("balance = %v, want 60 (stale reading must not restore the spent 40)", bal)
	}
}

// Snapshot copies must not alias ledger state; run with -race.
func TestSnapshotIsIndependentUnderConcurrency(t *testing.T) {
	l, err := OpenPools("", twoPools)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			id := "task-" + strings.Repeat("x", i%7) + string(rune('a'+i%26))
			_, _ = l.DebitMany(id, map[string]float64{"tauopt_suggestions": 1, "openrouter_usd": 0.1})
			_, _, _ = l.SettleTask("tauopt_suggestions", id, 0.5)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			_, _ = l.Reconcile("tauopt_suggestions", l.SettleEpoch("tauopt_suggestions"), External{Available: float64(900 + i), Cap: 1000, HasCap: true})
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			snap := l.Snapshot()
			for _, pool := range snap.Pools {
				pool.Balance = -1
				pool.Tasks["mutated-by-reader"] = TaskEntry{Reserved: 1}
			}
		}
	}()
	wg.Wait()
	for name, pool := range l.Snapshot().Pools {
		if pool.Balance < 0 {
			t.Fatalf("pool %s balance was changed through a snapshot copy", name)
		}
		if _, ok := pool.Tasks["mutated-by-reader"]; ok {
			t.Fatalf("pool %s tasks were changed through a snapshot copy", name)
		}
	}
}

func TestSettleAfterReconcileBelowReservations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	specs := []PoolSpec{{Resource: "q", Initial: 100, Cap: 100}}
	l, err := OpenPools(path, specs)
	if err != nil {
		t.Fatal(err)
	}
	reserve(t, l, "q", "a", 40)
	reserve(t, l, "q", "b", 20)

	// External: 10 remaining against 60 reserved and unsettled. The balance
	// is floored at zero and the uncovered 50 is kept as the deficit.
	if _, err := l.Reconcile("q", l.SettleEpoch("q"), External{Available: 10}); err != nil {
		t.Fatal(err)
	}
	if got := l.Snapshot().Pools["q"]; got.Balance != 0 || got.Deficit != 50 {
		t.Fatalf("pool after reconcile = %+v, want balance 0 deficit 50", got)
	}

	// The deficit is checkpointed, so a restart does not release it.
	l, err = OpenPools(path, specs)
	if err != nil {
		t.Fatal(err)
	}

	// Releasing 40 unused pays the deficit down to 10; nothing is available.
	if _, bal, err := l.SettleTask("q", "a", 0); err != nil || bal != 0 {
		t.Fatalf("SettleTask(a) = balance %v, err %v; want 0, nil", bal, err)
	}
	// Releasing 15 more clears the remaining 10 and leaves 5: the external 10
	// less the 5 that task b spent.
	if _, bal, err := l.SettleTask("q", "b", 5); err != nil || bal != 5 {
		t.Fatalf("SettleTask(b) = balance %v, err %v; want 5, nil", bal, err)
	}
	if got := l.Snapshot().Pools["q"]; got.Deficit != 0 {
		t.Fatalf("deficit after settlements = %v, want 0", got.Deficit)
	}
}

func TestSettleReleasesOnlyWhatExternalBalanceCovers(t *testing.T) {
	l, err := OpenPools("", []PoolSpec{{Resource: "q", Initial: 100, Cap: 100}})
	if err != nil {
		t.Fatal(err)
	}
	reserve(t, l, "q", "a", 40)
	if bal := mustBalance(t, l, "q"); bal != 60 {
		t.Fatalf("balance after reserve = %v, want 60", bal)
	}

	// External spending elsewhere leaves 10 remaining; 40 is still reserved.
	if _, err := l.Reconcile("q", l.SettleEpoch("q"), External{Available: 10}); err != nil {
		t.Fatal(err)
	}
	if bal := mustBalance(t, l, "q"); bal != 0 {
		t.Fatalf("balance after reconcile = %v, want 0", bal)
	}

	// The task spent nothing. Only the 10 the external balance holds is
	// released, not the 40 reserved.
	delta, bal, err := l.SettleTask("q", "a", 0)
	if err != nil || delta != 40 || bal != 10 {
		t.Fatalf("SettleTask = (delta %v, balance %v, %v), want (40, 10, nil)", delta, bal, err)
	}
}

func TestSettleAfterShortfallDebitAndRefill(t *testing.T) {
	l, err := OpenPools("", []PoolSpec{{Resource: "q", Initial: 50, Cap: 50}})
	if err != nil {
		t.Fatal(err)
	}
	// A launched task takes 75 from a pool holding 50: deficit 25.
	if _, err := l.DebitMany("a", map[string]float64{"q": 75}); err != nil {
		t.Fatal(err)
	}
	if got := l.Snapshot().Pools["q"]; got.Balance != 0 || got.Deficit != 25 {
		t.Fatalf("pool after debit = %+v, want balance 0 deficit 25", got)
	}
	// A refill is available at once and is not taken by the deficit.
	if bal, _, err := l.Refill("q", 1.5); err != nil || bal != 1.5 {
		t.Fatalf("Refill = balance %v, err %v; want 1.5, nil", bal, err)
	}
	// Releasing 5 pays the deficit down and does not lower the balance.
	if _, bal, err := l.SettleTask("q", "a", 70); err != nil || bal != 1.5 {
		t.Fatalf("SettleTask = balance %v, err %v; want 1.5, nil", bal, err)
	}
	if got := l.Snapshot().Pools["q"]; got.Deficit != 20 {
		t.Fatalf("deficit after settlement = %v, want 20", got.Deficit)
	}
}
