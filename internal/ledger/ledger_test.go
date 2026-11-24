package ledger

import (
	"path/filepath"
	"testing"
)

func TestLedgerReserveSettleRefillAndCheckpoint(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "ledger.json")

	l, err := Open(stateFile, "token_budget", 100, 100)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := l.SetProviderID("rp-1"); err != nil {
		t.Fatalf("SetProviderID: %v", err)
	}

	// Debit once; duplicate debit is a no-op.
	bal, changed, err := l.DebitTask("task-1", 25)
	if err != nil || !changed || bal != 75 {
		t.Fatalf("DebitTask #1 = (%v, %v, %v), want (75, true, nil)", bal, changed, err)
	}
	bal, changed, err = l.DebitTask("task-1", 25)
	if err != nil || changed || bal != 75 {
		t.Fatalf("DebitTask duplicate = (%v, %v, %v), want (75, false, nil)", bal, changed, err)
	}

	// Settle task-1 with actual=18 -> releases 7 unused back to 82.
	delta, bal, err := l.SettleTask("task-1", 18)
	if err != nil || delta != 7 || bal != 82 {
		t.Fatalf("SettleTask = (%v, %v, %v), want (7, 82, nil)", delta, bal, err)
	}
	// Duplicate settle is idempotent.
	delta, bal, err = l.SettleTask("task-1", 18)
	if err != nil || delta != 0 || bal != 82 {
		t.Fatalf("SettleTask duplicate = (%v, %v, %v), want (0, 82, nil)", delta, bal, err)
	}

	// Refill clamps at cap=100.
	bal, changed, err = l.Refill(50)
	if err != nil || !changed || bal != 100 {
		t.Fatalf("Refill = (%v, %v, %v), want (100, true, nil)", bal, changed, err)
	}

	// Debit down to 40, then reopen from checkpoint and verify survival.
	if _, _, err := l.DebitTask("task-2", 60); err != nil {
		t.Fatalf("DebitTask task-2: %v", err)
	}

	reopened, err := Open(stateFile, "token_budget", 100, 100)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if reopened.ProviderID() != "rp-1" {
		t.Fatalf("reopened ProviderID = %q, want rp-1", reopened.ProviderID())
	}
	if reopened.Balance() != 40 {
		t.Fatalf("reopened Balance = %v, want 40", reopened.Balance())
	}
	// Previously debited task stays deduplicated after restart.
	if bal, changed, _ := reopened.DebitTask("task-2", 60); changed || bal != 40 {
		t.Fatalf("post-restart duplicate debit = (%v, %v), want (40, false)", bal, changed)
	}
}
