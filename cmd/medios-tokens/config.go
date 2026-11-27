// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/medios-lab/medios-tokens/internal/balancesync"
	"github.com/medios-lab/medios-tokens/internal/ledger"
	"github.com/medios-lab/medios-tokens/internal/provider"
)

// provisionerOpenRouter is the only value a pool's "provisioner" field accepts.
const provisionerOpenRouter = "openrouter"

// fileConfig is the document read from --config.
type fileConfig struct {
	Pools []poolConfig `json:"pools"`
}

type poolConfig struct {
	// Resource is the scalar resource name advertised to Medios.
	Resource string `json:"resource"`
	// Initial and Cap seed the pool when the checkpoint does not hold it.
	Initial float64 `json:"initial"`
	Cap     float64 `json:"cap"`
	// RefillAmount is added on each refill trigger. Zero or omitted means
	// triggers leave the pool alone.
	RefillAmount float64 `json:"refill_amount"`
	// Provisioner, when "openrouter", binds per-task key provisioning and
	// terminal settlement to this pool.
	Provisioner string `json:"provisioner"`
	// Sync, when present, makes the pool's balance follow an HTTP endpoint.
	Sync *syncConfig `json:"sync"`
}

type syncConfig struct {
	URL string `json:"url"`
	// AuthHeader, AuthPrefix and AuthEnv describe the credential header:
	// "<AuthHeader>: <AuthPrefix><value of $AuthEnv>". AuthHeader and AuthEnv
	// are set together; omit all three for an endpoint that needs none.
	AuthHeader string `json:"auth_header"`
	AuthPrefix string `json:"auth_prefix"`
	AuthEnv    string `json:"auth_env"`
	// Interval and Timeout are Go duration strings such as "60s". Omitted
	// values take balancesync.DefaultInterval and balancesync.DefaultTimeout.
	Interval      string `json:"interval"`
	Timeout       string `json:"timeout"`
	CapPath       string `json:"cap_path"`
	UsedPath      string `json:"used_path"`
	RemainingPath string `json:"remaining_path"`
}

// poolSetup is the validated form of the pool configuration, from either
// --config or the single-pool flags.
type poolSetup struct {
	specs   []ledger.PoolSpec
	options map[string]provider.PoolOptions
	// provisionerResource is the pool bound to the key provisioner, or empty.
	provisionerResource string
	// summaries holds one log line per pool describing its effective settings.
	summaries []string
}

// loadPoolConfig reads and validates the --config file. lookupEnv resolves
// the environment variables that hold sync credentials.
func loadPoolConfig(path string, lookupEnv func(string) (string, bool)) (*poolSetup, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return parsePoolConfig(data, lookupEnv)
}

func parsePoolConfig(data []byte, lookupEnv func(string) (string, bool)) (*poolSetup, error) {
	var fc fileConfig
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&fc); err != nil {
		return nil, fmt.Errorf("parsing pool config: %w", err)
	}
	if len(fc.Pools) == 0 {
		return nil, errors.New("pool config declares no pools")
	}
	setup := &poolSetup{options: make(map[string]provider.PoolOptions)}
	for i, pc := range fc.Pools {
		if pc.Resource == "" {
			return nil, fmt.Errorf("pools[%d]: resource is required", i)
		}
		if _, dup := setup.options[pc.Resource]; dup {
			return nil, fmt.Errorf("pool %q is declared twice", pc.Resource)
		}
		if pc.Initial < 0 || pc.Cap < 0 || pc.RefillAmount < 0 {
			return nil, fmt.Errorf("pool %q: initial, cap and refill_amount must be non-negative", pc.Resource)
		}
		switch pc.Provisioner {
		case "":
		case provisionerOpenRouter:
			if setup.provisionerResource != "" {
				return nil, fmt.Errorf("pools %q and %q both set provisioner; one pool may", setup.provisionerResource, pc.Resource)
			}
			setup.provisionerResource = pc.Resource
		default:
			return nil, fmt.Errorf("pool %q: unknown provisioner %q (supported: %q)", pc.Resource, pc.Provisioner, provisionerOpenRouter)
		}

		opts := provider.PoolOptions{RefillAmount: pc.RefillAmount}
		summary := fmt.Sprintf("pool %s initial=%.6g cap=%.6g refill_amount=%.6g", pc.Resource, pc.Initial, pc.Cap, pc.RefillAmount)
		if pc.Provisioner != "" {
			summary += " provisioner=" + pc.Provisioner
		}
		if pc.Sync != nil {
			if pc.RefillAmount > 0 {
				return nil, fmt.Errorf("pool %q: refill_amount cannot be combined with sync; the synced balance would overwrite refills", pc.Resource)
			}
			poller, interval, desc, err := buildPoller(*pc.Sync, lookupEnv)
			if err != nil {
				return nil, fmt.Errorf("pool %q: %w", pc.Resource, err)
			}
			opts.Source = poller
			opts.SyncInterval = interval
			summary += " " + desc
		}
		setup.specs = append(setup.specs, ledger.PoolSpec{Resource: pc.Resource, Initial: pc.Initial, Cap: pc.Cap})
		setup.options[pc.Resource] = opts
		setup.summaries = append(setup.summaries, summary)
	}
	return setup, nil
}

// buildPoller turns one sync block into a poller and its interval. The
// returned description names the credential's environment variable, never
// its value.
func buildPoller(sc syncConfig, lookupEnv func(string) (string, bool)) (*balancesync.Poller, time.Duration, string, error) {
	interval, err := durationOrDefault("interval", sc.Interval, balancesync.DefaultInterval)
	if err != nil {
		return nil, 0, "", err
	}
	timeout, err := durationOrDefault("timeout", sc.Timeout, balancesync.DefaultTimeout)
	if err != nil {
		return nil, 0, "", err
	}

	cfg := balancesync.Config{
		URL:           sc.URL,
		Timeout:       timeout,
		CapPath:       sc.CapPath,
		UsedPath:      sc.UsedPath,
		RemainingPath: sc.RemainingPath,
	}
	auth := "auth=none"
	switch {
	case sc.AuthEnv == "" && (sc.AuthHeader != "" || sc.AuthPrefix != ""):
		return nil, 0, "", errors.New("sync sets auth_header or auth_prefix without auth_env")
	case sc.AuthEnv != "" && sc.AuthHeader == "":
		return nil, 0, "", errors.New("sync sets auth_env without auth_header")
	case sc.AuthEnv != "":
		token, ok := lookupEnv(sc.AuthEnv)
		if !ok || token == "" {
			return nil, 0, "", fmt.Errorf("sync credential: environment variable %s is unset or empty", sc.AuthEnv)
		}
		cfg.AuthHeader = sc.AuthHeader
		cfg.AuthValue = sc.AuthPrefix + token
		auth = fmt.Sprintf("auth_header=%s auth_env=%s", sc.AuthHeader, sc.AuthEnv)
	}

	poller, err := balancesync.New(cfg)
	if err != nil {
		return nil, 0, "", err
	}
	desc := fmt.Sprintf("sync=%s interval=%s timeout=%s %s", poller.Endpoint(), interval, timeout, auth)
	return poller, interval, desc, nil
}

func durationOrDefault(field, value string, def time.Duration) (time.Duration, error) {
	if value == "" {
		return def, nil
	}
	d, err := time.ParseDuration(value)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("sync %s %q is not a positive duration", field, value)
	}
	return d, nil
}
