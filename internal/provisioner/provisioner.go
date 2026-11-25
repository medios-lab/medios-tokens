// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package provisioner

import "context"

// KeyGrant is an ephemeral per-task API key capped at Limit.
// Hash is an opaque provider handle safe to checkpoint in the ledger;
// Secret is returned once to the caller and must never be persisted to disk.
type KeyGrant struct {
	Hash   string  `json:"key_hash"`
	Secret string  `json:"key"`
	Limit  float64 `json:"limit"`
}

// Provisioner mints hard-capped per-task API keys and settles/revokes them
// once a task reaches a terminal state.
type Provisioner interface {
	Provision(ctx context.Context, taskID string, limit float64) (KeyGrant, error)
	SettleAndRevoke(ctx context.Context, taskID string, keyHash string) (actual float64, err error)
}
