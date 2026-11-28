// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

// Package balancesync reads a pool's balance from an HTTP endpoint that
// returns JSON.
package balancesync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/medios-lab/medios-tokens/internal/ledger"
)

const (
	// DefaultInterval is the time between polls of one pool when its
	// configuration gives none. Default, provisional: one request per minute
	// bounds how long an advertised balance can lag external spend to about
	// 60 seconds; no measurement of any quota API's rate limit supports it.
	DefaultInterval = 60 * time.Second

	// DefaultTimeout bounds one poll, from connect through reading the body,
	// when the configuration gives none. Default, provisional: 10 seconds is
	// shorter than DefaultInterval so a hung endpoint costs at most one
	// interval; it is not derived from measured endpoint latency.
	DefaultTimeout = 10 * time.Second

	// maxResponseBytes is the largest response body a poll reads. Invariant:
	// a larger body is an error, so a misbehaving endpoint cannot grow the
	// provider's memory. 1 MiB is far above the size of a quota document.
	maxResponseBytes = 1 << 20
)

// Config describes one pool's balance endpoint.
//
// The balance comes from either CapPath and UsedPath (available = cap - used)
// or RemainingPath (available = remaining, with CapPath optional). Paths are
// dot-separated object keys, for example "org.usage.suggests_24h".
//
// AuthHeader and AuthValue are set together or both left empty. AuthValue is
// the complete header value, prefix included.
type Config struct {
	URL           string
	AuthHeader    string
	AuthValue     string
	Timeout       time.Duration
	CapPath       string
	UsedPath      string
	RemainingPath string
}

// Poller fetches one balance per call to Fetch. It holds no schedule.
type Poller struct {
	cfg      Config
	endpoint string
	client   *http.Client
}

// New validates cfg and returns a Poller.
func New(cfg Config) (*Poller, error) {
	u, err := url.Parse(cfg.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, errors.New("sync url must be an absolute http or https URL")
	}
	if (cfg.AuthHeader == "") != (cfg.AuthValue == "") {
		return nil, errors.New("sync auth header and credential must be set together")
	}
	if cfg.Timeout <= 0 {
		return nil, errors.New("sync timeout must be positive")
	}
	switch {
	case cfg.RemainingPath != "" && cfg.UsedPath != "":
		return nil, errors.New("sync takes used_path or remaining_path, not both")
	case cfg.RemainingPath == "" && (cfg.CapPath == "" || cfg.UsedPath == ""):
		return nil, errors.New("sync needs cap_path with used_path, or remaining_path")
	}
	return &Poller{
		cfg:      cfg,
		endpoint: u.Scheme + "://" + u.Host + u.Path,
		client: &http.Client{
			// Redirects are not followed: a redirect to another host would
			// carry the credential header there.
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}, nil
}

// Endpoint returns the polled URL without its query string or user
// information, for logging.
func (p *Poller) Endpoint() string {
	return p.endpoint
}

// Fetch performs one GET and extracts the balance. Errors never contain the
// request URL's query, the credential, or the response body.
func (p *Poller) Fetch(ctx context.Context) (ledger.External, error) {
	ctx, cancel := context.WithTimeout(ctx, p.cfg.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.cfg.URL, nil)
	if err != nil {
		return ledger.External{}, errors.New("building request failed")
	}
	req.Header.Set("Accept", "application/json")
	if p.cfg.AuthHeader != "" {
		req.Header.Set(p.cfg.AuthHeader, p.cfg.AuthValue)
	}
	resp, err := p.client.Do(req)
	if err != nil {
		// *url.Error prints the full request URL; keep only its cause.
		var ue *url.Error
		if errors.As(err, &ue) {
			err = ue.Err
		}
		return ledger.External{}, fmt.Errorf("GET %s: %w", p.endpoint, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return ledger.External{}, fmt.Errorf("GET %s: HTTP %d", p.endpoint, resp.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil {
		return ledger.External{}, fmt.Errorf("GET %s: reading body: %w", p.endpoint, err)
	}
	if len(raw) > maxResponseBytes {
		return ledger.External{}, fmt.Errorf("GET %s: body exceeds %d bytes", p.endpoint, maxResponseBytes)
	}
	var doc any
	if err := json.Unmarshal(raw, &doc); err != nil {
		return ledger.External{}, fmt.Errorf("GET %s: body is not JSON", p.endpoint)
	}

	var out ledger.External
	if p.cfg.CapPath != "" {
		if out.Cap, err = lookupNumber(doc, p.cfg.CapPath); err != nil {
			return ledger.External{}, err
		}
		out.HasCap = true
	}
	if p.cfg.RemainingPath != "" {
		if out.Available, err = lookupNumber(doc, p.cfg.RemainingPath); err != nil {
			return ledger.External{}, err
		}
		return out, nil
	}
	used, err := lookupNumber(doc, p.cfg.UsedPath)
	if err != nil {
		return ledger.External{}, err
	}
	out.Available = math.Max(0, out.Cap-used)
	return out, nil
}

// lookupNumber walks dot-separated object keys from doc and returns the
// non-negative JSON number found there.
func lookupNumber(doc any, path string) (float64, error) {
	cur := doc
	for _, key := range strings.Split(path, ".") {
		obj, ok := cur.(map[string]any)
		if !ok {
			return 0, fmt.Errorf("path %q: %q is not inside a JSON object", path, key)
		}
		cur, ok = obj[key]
		if !ok {
			return 0, fmt.Errorf("path %q: key %q is missing", path, key)
		}
	}
	n, ok := cur.(float64)
	if !ok {
		return 0, fmt.Errorf("path %q: value is not a JSON number", path)
	}
	if n < 0 {
		return 0, fmt.Errorf("path %q: value is negative", path)
	}
	return n, nil
}
