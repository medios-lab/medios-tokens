// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package ledger

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// SnapshotVersion is the checkpoint format written by this package: a map of
// named pools. Checkpoints without a "pools" object are read as the earlier
// single-pool format and converted on load.
const SnapshotVersion = 2

var (
	// ErrInsufficient is returned by ReserveMany when at least one requested
	// pool holds less than the requested amount. No pool is debited.
	ErrInsufficient = errors.New("insufficient balance")

	// ErrUnknownPool is returned when a resource name matches no pool.
	ErrUnknownPool = errors.New("unknown pool")

	// ErrStaleReading is returned by Reconcile when a task in the pool was
	// settled after the caller sampled SettleEpoch. The external reading may
	// not include that task's spend, so applying it could over-state the
	// balance.
	ErrStaleReading = errors.New("external reading predates a settlement")
)

// TaskEntry records one task's reserved and settled consumable amount.
// KeyHash is an optional opaque provider key identifier (never a raw secret).
// Released marks a reservation closed by ReleaseTask: the ledger was not told
// the task's spend, so Actual is zero and does not describe it.
type TaskEntry struct {
	Reserved float64 `json:"reserved"`
	Settled  bool    `json:"settled"`
	Actual   float64 `json:"actual,omitempty"`
	Released bool    `json:"released,omitempty"`
	KeyHash  string  `json:"key_hash,omitempty"`
}

// PoolSnapshot is the state of one named consumable scalar.
//
// Deficit is the amount by which unsettled reservations exceed what the pool
// can cover. It arises when an external reading falls below the unsettled
// reservations or when a launched task is debited beyond the balance. Credit
// released by a settlement pays the deficit down before it raises Balance. A
// refill adds to Balance and leaves the deficit, so both can be non-zero.
type PoolSnapshot struct {
	Balance float64              `json:"balance"`
	Cap     float64              `json:"cap"`
	Deficit float64              `json:"deficit,omitempty"`
	Frozen  bool                 `json:"frozen,omitempty"`
	Tasks   map[string]TaskEntry `json:"tasks"`
}

// setPosition stores a signed position (available amount less uncovered
// reservations) as Balance, clamped at Cap, and Deficit.
func (p *PoolSnapshot) setPosition(net float64) {
	net = round6(net)
	if net >= 0 {
		p.Balance = math.Min(p.Cap, net)
		p.Deficit = 0
		return
	}
	p.Balance = 0
	p.Deficit = -net
}

// Snapshot is the serializable checkpoint of the provider's budget ledger.
type Snapshot struct {
	Version    int                      `json:"version"`
	ProviderID string                   `json:"provider_id,omitempty"`
	Pools      map[string]*PoolSnapshot `json:"pools"`
}

// PoolSpec declares one pool and the balance and cap it starts with when the
// checkpoint does not already contain it.
type PoolSpec struct {
	Resource string
	Initial  float64
	Cap      float64
}

// External is a balance read from the system that owns a pool's quota.
// Available is the amount the external system reports as not yet consumed.
// Cap is meaningful only when HasCap is true.
type External struct {
	Available float64
	Cap       float64
	HasCap    bool
}

// Ledger tracks consumable capacity for a fixed set of named pools with
// optional atomic JSON checkpointing.
// Lifecycle per task: ReserveMany/DebitMany (on task admission) -> SettleTask
// (reconcile actual spend and release unused credit) -> Refill or Reconcile
// (replenishment).
//
// Every mutation is applied to a copy of the snapshot, written to the
// checkpoint, and installed in memory only after the write succeeds, so a
// failed write leaves memory and checkpoint identical.
type Ledger struct {
	mu        sync.Mutex
	statePath string
	snap      Snapshot
	// settleSeq counts settlements per pool since process start. It is not
	// checkpointed: it only orders settlements against in-flight external
	// reads within one process.
	settleSeq map[string]uint64
}

func round6(v float64) float64 {
	return math.Round(v*1e6) / 1e6
}

// checkpointFile decodes both checkpoint formats. The single-pool format
// stores resource, balance, cap, frozen and tasks at the top level.
type checkpointFile struct {
	Version    int                      `json:"version"`
	ProviderID string                   `json:"provider_id"`
	Pools      map[string]*PoolSnapshot `json:"pools"`

	Resource string               `json:"resource"`
	Balance  float64              `json:"balance"`
	Cap      float64              `json:"cap"`
	Frozen   bool                 `json:"frozen"`
	Tasks    map[string]TaskEntry `json:"tasks"`
}

func decodeCheckpoint(data []byte) (Snapshot, error) {
	var f checkpointFile
	if err := json.Unmarshal(data, &f); err != nil {
		return Snapshot{}, err
	}
	snap := Snapshot{Version: SnapshotVersion, ProviderID: f.ProviderID, Pools: map[string]*PoolSnapshot{}}
	if f.Pools == nil {
		if f.Version > 1 {
			return Snapshot{}, fmt.Errorf("version %d checkpoint has no pools", f.Version)
		}
		if f.Resource == "" {
			return Snapshot{}, errors.New("single-pool checkpoint has no resource name")
		}
		tasks := f.Tasks
		if tasks == nil {
			tasks = make(map[string]TaskEntry)
		}
		snap.Pools[f.Resource] = &PoolSnapshot{Balance: f.Balance, Cap: f.Cap, Frozen: f.Frozen, Tasks: tasks}
		return snap, nil
	}
	if f.Version != SnapshotVersion {
		return Snapshot{}, fmt.Errorf("unsupported checkpoint version %d (want %d)", f.Version, SnapshotVersion)
	}
	for name, pool := range f.Pools {
		if name == "" || pool == nil {
			return Snapshot{}, fmt.Errorf("pool %q is empty or unnamed", name)
		}
		if pool.Tasks == nil {
			pool.Tasks = make(map[string]TaskEntry)
		}
		snap.Pools[name] = pool
	}
	return snap, nil
}

func cloneSnapshot(s Snapshot) Snapshot {
	cp := Snapshot{Version: s.Version, ProviderID: s.ProviderID, Pools: make(map[string]*PoolSnapshot, len(s.Pools))}
	for name, pool := range s.Pools {
		pc := *pool
		pc.Tasks = make(map[string]TaskEntry, len(pool.Tasks))
		for id, entry := range pool.Tasks {
			pc.Tasks[id] = entry
		}
		cp.Pools[name] = &pc
	}
	return cp
}

// Open is OpenPools for a single pool.
func Open(statePath, resource string, initial, capVal float64) (*Ledger, error) {
	return OpenPools(statePath, []PoolSpec{{Resource: resource, Initial: initial, Cap: capVal}})
}

// OpenPools loads an existing checkpoint from statePath if present, or
// initializes a fresh ledger from specs. Empty statePath runs in-memory.
//
// When a checkpoint exists, its pools keep their checkpointed state and any
// spec it does not contain is added with the spec's initial balance and cap.
// A checkpoint pool that no spec names is an error: dropping it would discard
// a balance and its unsettled reservations.
func OpenPools(statePath string, specs []PoolSpec) (*Ledger, error) {
	if len(specs) == 0 {
		return nil, errors.New("at least one pool is required")
	}
	fresh := make(map[string]*PoolSnapshot, len(specs))
	for _, spec := range specs {
		if spec.Resource == "" {
			return nil, errors.New("pool resource name is required")
		}
		if _, dup := fresh[spec.Resource]; dup {
			return nil, fmt.Errorf("pool %q is declared twice", spec.Resource)
		}
		if spec.Initial < 0 || spec.Cap < 0 {
			return nil, fmt.Errorf("pool %q: initial balance and cap must be non-negative", spec.Resource)
		}
		capVal := spec.Cap
		if capVal < spec.Initial {
			capVal = spec.Initial
		}
		fresh[spec.Resource] = &PoolSnapshot{
			Balance: round6(spec.Initial),
			Cap:     round6(capVal),
			Tasks:   make(map[string]TaskEntry),
		}
	}
	l := &Ledger{
		statePath: statePath,
		snap:      Snapshot{Version: SnapshotVersion, Pools: fresh},
		settleSeq: make(map[string]uint64),
	}
	if statePath == "" {
		return l, nil
	}
	data, err := os.ReadFile(statePath)
	if os.IsNotExist(err) {
		if err := l.persist(l.snap); err != nil {
			return nil, err
		}
		return l, nil
	}
	if err != nil {
		return nil, fmt.Errorf("reading ledger checkpoint %s: %w", statePath, err)
	}
	loaded, err := decodeCheckpoint(data)
	if err != nil {
		return nil, fmt.Errorf("corrupt ledger checkpoint %s: %w", statePath, err)
	}
	for name := range loaded.Pools {
		if _, ok := fresh[name]; !ok {
			return nil, fmt.Errorf("ledger checkpoint %s holds pool %q, which is not configured", statePath, name)
		}
	}
	for name, pool := range fresh {
		if _, ok := loaded.Pools[name]; !ok {
			loaded.Pools[name] = pool
		}
	}
	l.snap = loaded
	return l, nil
}

func (l *Ledger) persist(s Snapshot) error {
	if l.statePath == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(l.statePath), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	tmp := l.statePath + ".tmp"
	if err := os.WriteFile(tmp, append(data, '\n'), 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, l.statePath)
}

// updateLocked applies fn to a copy of the snapshot and installs the copy
// only if fn and the checkpoint write both succeed. l.mu must be held.
func (l *Ledger) updateLocked(fn func(s *Snapshot) error) error {
	next := cloneSnapshot(l.snap)
	if err := fn(&next); err != nil {
		return err
	}
	if err := l.persist(next); err != nil {
		return err
	}
	l.snap = next
	return nil
}

func (l *Ledger) poolLocked(resource string) (*PoolSnapshot, error) {
	pool, ok := l.snap.Pools[resource]
	if !ok {
		return nil, fmt.Errorf("%w %q", ErrUnknownPool, resource)
	}
	return pool, nil
}

// Snapshot returns a deep copy of the ledger state.
func (l *Ledger) Snapshot() Snapshot {
	l.mu.Lock()
	defer l.mu.Unlock()
	return cloneSnapshot(l.snap)
}

// Resources returns the pool names in sorted order. The set is fixed at open.
func (l *Ledger) Resources() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	names := make([]string, 0, len(l.snap.Pools))
	for name := range l.snap.Pools {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (l *Ledger) ProviderID() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.snap.ProviderID
}

func (l *Ledger) SetProviderID(id string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.snap.ProviderID == id {
		return nil
	}
	return l.updateLocked(func(s *Snapshot) error {
		s.ProviderID = id
		return nil
	})
}

// Balance returns the current balance of one pool.
func (l *Ledger) Balance(resource string) (float64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	pool, err := l.poolLocked(resource)
	if err != nil {
		return 0, err
	}
	return pool.Balance, nil
}

// Balances returns the current balance of every pool, read under one lock.
func (l *Ledger) Balances() map[string]float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make(map[string]float64, len(l.snap.Pools))
	for name, pool := range l.snap.Pools {
		out[name] = pool.Balance
	}
	return out
}

// CanAuthorize reports whether a PUBLISH_RESOURCES gate may proceed for the
// named pools: none of them may be frozen. Names that match no pool are
// ignored. With no names, every pool is checked.
func (l *Ledger) CanAuthorize(resources []string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(resources) == 0 {
		for _, pool := range l.snap.Pools {
			if pool.Frozen {
				return false
			}
		}
		return true
	}
	for _, name := range resources {
		if pool, ok := l.snap.Pools[name]; ok && pool.Frozen {
			return false
		}
	}
	return true
}

// RecordKeyHash associates an opaque provisioned key handle (never a raw secret)
// with taskID in one pool and checkpoints the ledger atomically.
func (l *Ledger) RecordKeyHash(resource, taskID, keyHash string) error {
	if taskID == "" || keyHash == "" {
		return errors.New("task_id and key_hash are required")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, err := l.poolLocked(resource); err != nil {
		return err
	}
	return l.updateLocked(func(s *Snapshot) error {
		pool := s.Pools[resource]
		entry := pool.Tasks[taskID]
		entry.KeyHash = keyHash
		pool.Tasks[taskID] = entry
		return nil
	})
}

// TaskKeyHash returns the recorded key hash and settlement state for taskID
// in one pool.
func (l *Ledger) TaskKeyHash(resource, taskID string) (keyHash string, settled bool, ok bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	pool, found := l.snap.Pools[resource]
	if !found {
		return "", false, false
	}
	entry, exists := pool.Tasks[taskID]
	if !exists || entry.KeyHash == "" {
		return "", false, false
	}
	return entry.KeyHash, entry.Settled, true
}

// ReserveMany debits every pool in reqs for taskID, or none of them. It
// returns ErrInsufficient when any pool holds less than its requested amount
// and ErrUnknownPool when a name matches no pool; in both cases no balance,
// task entry or checkpoint byte changes.
//
// A task is debited once: if taskID already holds a reservation in any
// requested pool the call changes nothing and returns (false, nil). Entries
// with a non-positive amount are skipped.
func (l *Ledger) ReserveMany(taskID string, reqs map[string]float64) (bool, error) {
	return l.debit(taskID, reqs, true)
}

// DebitMany records taskID's reservation in every pool in reqs and reduces
// each balance by the requested amount. Where a pool holds less, its balance
// drops to zero and the shortfall is added to its Deficit. It is for a task
// the broker has already launched: the reservation must be recorded even when
// the pool cannot cover it. Deduplication and
// atomicity are the same as ReserveMany.
func (l *Ledger) DebitMany(taskID string, reqs map[string]float64) (bool, error) {
	return l.debit(taskID, reqs, false)
}

func (l *Ledger) debit(taskID string, reqs map[string]float64, strict bool) (bool, error) {
	if taskID == "" {
		return false, nil
	}
	amounts := make(map[string]float64, len(reqs))
	for name, amount := range reqs {
		if amount > 0 {
			amounts[name] = round6(amount)
		}
	}
	if len(amounts) == 0 {
		return false, nil
	}
	names := make([]string, 0, len(amounts))
	for name := range amounts {
		names = append(names, name)
	}
	sort.Strings(names)

	l.mu.Lock()
	defer l.mu.Unlock()
	for _, name := range names {
		pool, err := l.poolLocked(name)
		if err != nil {
			return false, err
		}
		if pool.Tasks[taskID].Reserved > 0 {
			return false, nil
		}
	}
	if strict {
		for _, name := range names {
			if have := l.snap.Pools[name].Balance; amounts[name] > have {
				return false, fmt.Errorf("%w: pool %q holds %.6g, task %q requests %.6g",
					ErrInsufficient, name, have, taskID, amounts[name])
			}
		}
	}
	err := l.updateLocked(func(s *Snapshot) error {
		for _, name := range names {
			pool := s.Pools[name]
			entry := pool.Tasks[taskID]
			entry.Reserved = amounts[name]
			pool.Tasks[taskID] = entry
			short := math.Max(0, round6(amounts[name]-pool.Balance))
			pool.Balance = math.Max(0, round6(pool.Balance-amounts[name]))
			pool.Deficit = round6(pool.Deficit + short)
		}
		return nil
	})
	if err != nil {
		return false, err
	}
	return true, nil
}

// SettleTask reconciles actual consumption for a previously debited task in
// one pool and releases any unused reservation (reserved - actual). The
// released amount first pays down the pool's Deficit; the rest raises the
// balance, clamped at Cap. Returns (deltaReleased, newBalance, err).
func (l *Ledger) SettleTask(resource, taskID string, actual float64) (float64, float64, error) {
	if actual < 0 {
		return 0, 0, errors.New("actual spend must be non-negative")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	pool, err := l.poolLocked(resource)
	if err != nil {
		return 0, 0, err
	}
	entry, ok := pool.Tasks[taskID]
	if !ok {
		return 0, pool.Balance, fmt.Errorf("unknown task_id %q in pool %q", taskID, resource)
	}
	if entry.Settled {
		return 0, pool.Balance, nil
	}
	return l.closeLocked(resource, taskID, entry, actual, false)
}

// ReleaseTask closes a task's open reservation in one pool without a spend
// figure and returns the whole reservation, as SettleTask does for an actual
// of zero. It is for a pool whose balance is reconciled against an external
// reading: the task's spend reaches the ledger through the next reading, and
// only the reservation has to stop being subtracted. A task with no entry in
// the pool, or one already settled, is left as it is and released is false.
// Returns (amountReleased, newBalance, released, err).
func (l *Ledger) ReleaseTask(resource, taskID string) (float64, float64, bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	pool, err := l.poolLocked(resource)
	if err != nil {
		return 0, 0, false, err
	}
	entry, ok := pool.Tasks[taskID]
	if !ok || entry.Settled {
		return 0, pool.Balance, false, nil
	}
	delta, bal, err := l.closeLocked(resource, taskID, entry, 0, true)
	return delta, bal, err == nil, err
}

// closeLocked marks an unsettled entry settled and applies reserved - actual
// to the pool. Caller holds l.mu.
func (l *Ledger) closeLocked(resource, taskID string, entry TaskEntry, actual float64, released bool) (float64, float64, error) {
	prev := l.snap.Pools[resource].Balance
	unused := round6(entry.Reserved - actual)
	err := l.updateLocked(func(s *Snapshot) error {
		p := s.Pools[resource]
		entry.Settled = true
		entry.Actual = round6(actual)
		entry.Released = released
		p.Tasks[taskID] = entry
		if unused < 0 {
			// Overrun: deduct any extra actual spend beyond the reservation, floored at 0.
			p.Balance = math.Max(0, round6(p.Balance+unused))
		} else if unused > 0 {
			pay := math.Min(unused, p.Deficit)
			p.Deficit = round6(p.Deficit - pay)
			p.Balance = math.Min(p.Cap, round6(p.Balance+unused-pay))
		}
		return nil
	})
	if err != nil {
		return 0, prev, err
	}
	l.settleSeq[resource]++
	return unused, l.snap.Pools[resource].Balance, nil
}

// Refill adds amount to one pool up to its Cap. Returns (newBalance, changed, err).
// A refill is a grant of new budget and leaves the pool's Deficit as it is.
func (l *Ledger) Refill(resource string, amount float64) (float64, bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	pool, err := l.poolLocked(resource)
	if err != nil {
		return 0, false, err
	}
	if amount <= 0 {
		return pool.Balance, false, nil
	}
	next := math.Min(pool.Cap, round6(pool.Balance+amount))
	if next == pool.Balance {
		return pool.Balance, false, nil
	}
	if err := l.updateLocked(func(s *Snapshot) error {
		s.Pools[resource].Balance = next
		return nil
	}); err != nil {
		return pool.Balance, false, err
	}
	return next, true, nil
}

// SettleEpoch returns a counter that advances each time a task in the pool is
// settled. Sample it before reading an external balance and pass it to
// Reconcile.
func (l *Ledger) SettleEpoch(resource string) uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.settleSeq[resource]
}

// Reconcile replaces one pool's balance with an externally read balance less
// the reservations that are not yet settled:
//
//	balance = max(0, ext.Available - sum(Reserved of unsettled tasks))
//	deficit = max(0, sum(Reserved of unsettled tasks) - ext.Available)
//
// The deficit keeps the part of the reservations the reading does not cover,
// so a later settlement releases only what the external balance allows.
// The unsettled sum is taken under the ledger lock at the time the reading is
// applied, so a reservation made while the external read was in flight is
// subtracted. epoch is the SettleEpoch sampled before the external read; if a
// settlement happened since, Reconcile changes nothing and returns
// ErrStaleReading, because that task has left the unsettled sum while the
// reading may not yet include its spend.
//
// Cap becomes ext.Cap when the reading carries one. Otherwise Cap is kept,
// and raised to ext.Available only when the external balance exceeds it.
// Returns whether balance, deficit or cap changed.
func (l *Ledger) Reconcile(resource string, epoch uint64, ext External) (bool, error) {
	if ext.Available < 0 || (ext.HasCap && ext.Cap < 0) {
		return false, errors.New("external balance and cap must be non-negative")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	pool, err := l.poolLocked(resource)
	if err != nil {
		return false, err
	}
	if l.settleSeq[resource] != epoch {
		return false, ErrStaleReading
	}
	var inflight float64
	for _, entry := range pool.Tasks {
		if !entry.Settled {
			inflight += entry.Reserved
		}
	}
	capVal := pool.Cap
	if ext.HasCap {
		capVal = round6(ext.Cap)
	} else if ext.Available > capVal {
		capVal = round6(ext.Available)
	}
	next := PoolSnapshot{Cap: capVal}
	next.setPosition(ext.Available - inflight)
	if next.Balance == pool.Balance && next.Deficit == pool.Deficit && capVal == pool.Cap {
		return false, nil
	}
	if err := l.updateLocked(func(s *Snapshot) error {
		s.Pools[resource].Balance = next.Balance
		s.Pools[resource].Deficit = next.Deficit
		s.Pools[resource].Cap = capVal
		return nil
	}); err != nil {
		return false, err
	}
	return true, nil
}
