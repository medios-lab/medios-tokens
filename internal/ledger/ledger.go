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
	"sync"
)

// TaskEntry records one task's reserved and settled consumable amount.
type TaskEntry struct {
	Reserved float64 `json:"reserved"`
	Settled  bool    `json:"settled"`
	Actual   float64 `json:"actual,omitempty"`
}

// Snapshot is the serializable checkpoint of the provider's budget ledger.
type Snapshot struct {
	ProviderID string               `json:"provider_id,omitempty"`
	Resource   string               `json:"resource"`
	Balance    float64              `json:"balance"`
	Cap        float64              `json:"cap"`
	Frozen     bool                 `json:"frozen,omitempty"`
	Tasks      map[string]TaskEntry `json:"tasks"`
}

// Ledger tracks consumable token capacity with optional atomic JSON checkpointing.
// Lifecycle: Reserve/Debit (on task admission) -> Settle (reconcile actual spend
// and release unused credit) -> Refill (external replenishment up to cap).
type Ledger struct {
	mu        sync.Mutex
	statePath string
	snap      Snapshot
}

func round6(v float64) float64 {
	return math.Round(v*1e6) / 1e6
}

// Open loads an existing checkpoint from statePath if present, or initializes a
// fresh ledger with initial balance and cap. Empty statePath runs in-memory.
func Open(statePath, resource string, initial, capVal float64) (*Ledger, error) {
	if initial < 0 || capVal < 0 {
		return nil, errors.New("initial balance and cap must be non-negative")
	}
	if capVal < initial {
		capVal = initial
	}
	l := &Ledger{
		statePath: statePath,
		snap: Snapshot{
			Resource: resource,
			Balance:  round6(initial),
			Cap:      round6(capVal),
			Tasks:    make(map[string]TaskEntry),
		},
	}
	if statePath != "" {
		data, err := os.ReadFile(statePath)
		if err == nil {
			var loaded Snapshot
			if err := json.Unmarshal(data, &loaded); err != nil {
				return nil, fmt.Errorf("corrupt ledger checkpoint %s: %w", statePath, err)
			}
			if loaded.Tasks == nil {
				loaded.Tasks = make(map[string]TaskEntry)
			}
			if loaded.Resource == "" {
				loaded.Resource = resource
			}
			l.snap = loaded
			return l, nil
		} else if !os.IsNotExist(err) {
			return nil, fmt.Errorf("reading ledger checkpoint %s: %w", statePath, err)
		}
		if err := l.persistLocked(); err != nil {
			return nil, err
		}
	}
	return l, nil
}

func (l *Ledger) persistLocked() error {
	if l.statePath == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(l.statePath), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(l.snap, "", "  ")
	if err != nil {
		return err
	}
	tmp := l.statePath + ".tmp"
	if err := os.WriteFile(tmp, append(data, '\n'), 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, l.statePath)
}

func (l *Ledger) Snapshot() Snapshot {
	l.mu.Lock()
	defer l.mu.Unlock()
	cp := l.snap
	cp.Tasks = make(map[string]TaskEntry, len(l.snap.Tasks))
	for k, v := range l.snap.Tasks {
		cp.Tasks[k] = v
	}
	return cp
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
	l.snap.ProviderID = id
	return l.persistLocked()
}

func (l *Ledger) Balance() float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.snap.Balance
}

// CanAuthorize reports whether a PUBLISH_RESOURCES gate may proceed.
func (l *Ledger) CanAuthorize() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return !l.snap.Frozen
}

// DebitTask records a task's reserved spend exactly once per unique taskID.
// Returns (balanceAfter, changed, err).
func (l *Ledger) DebitTask(taskID string, amount float64) (float64, bool, error) {
	if taskID == "" || amount <= 0 {
		return l.Balance(), false, nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, exists := l.snap.Tasks[taskID]; exists {
		return l.snap.Balance, false, nil
	}
	l.snap.Tasks[taskID] = TaskEntry{Reserved: round6(amount)}
	next := round6(l.snap.Balance - amount)
	if next < 0 {
		next = 0
	}
	l.snap.Balance = next
	if err := l.persistLocked(); err != nil {
		return l.snap.Balance, false, err
	}
	return l.snap.Balance, true, nil
}

// SettleTask reconciles actual consumption for a previously debited task and
// releases any unused reservation (reserved - actual) back to the balance,
// clamped at Cap. Returns (deltaReleased, newBalance, err).
func (l *Ledger) SettleTask(taskID string, actual float64) (float64, float64, error) {
	if actual < 0 {
		return 0, 0, errors.New("actual spend must be non-negative")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	entry, ok := l.snap.Tasks[taskID]
	if !ok {
		return 0, l.snap.Balance, fmt.Errorf("unknown task_id %q", taskID)
	}
	if entry.Settled {
		return 0, l.snap.Balance, nil
	}
	entry.Settled = true
	entry.Actual = round6(actual)
	l.snap.Tasks[taskID] = entry

	unused := round6(entry.Reserved - actual)
	if unused < 0 {
		// Overrun: deduct any extra actual spend beyond the reservation, floored at 0.
		l.snap.Balance = math.Max(0, round6(l.snap.Balance+unused))
	} else if unused > 0 {
		l.snap.Balance = math.Min(l.snap.Cap, round6(l.snap.Balance+unused))
	}
	if err := l.persistLocked(); err != nil {
		return 0, l.snap.Balance, err
	}
	return unused, l.snap.Balance, nil
}

// Refill adds amount up to Cap. Returns (newBalance, changed, err).
func (l *Ledger) Refill(amount float64) (float64, bool, error) {
	if amount <= 0 {
		return l.Balance(), false, nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	before := l.snap.Balance
	l.snap.Balance = math.Min(l.snap.Cap, round6(l.snap.Balance+amount))
	if l.snap.Balance == before {
		return l.snap.Balance, false, nil
	}
	if err := l.persistLocked(); err != nil {
		return l.snap.Balance, false, err
	}
	return l.snap.Balance, true, nil
}
