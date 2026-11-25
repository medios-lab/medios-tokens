// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package provisioner

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const DefaultOpenRouterBaseURL = "https://openrouter.ai"

// OpenRouter implements Provisioner using OpenRouter's Provisioning Keys API
// (POST /api/v1/keys, GET /api/v1/keys/{hash}, DELETE /api/v1/keys/{hash}).
type OpenRouter struct {
	baseURL         string
	provisioningKey string
	client          *http.Client
}

func NewOpenRouter(provisioningKey, baseURL string, client *http.Client) (*OpenRouter, error) {
	if strings.TrimSpace(provisioningKey) == "" {
		return nil, errors.New("openrouter provisioning key is required")
	}
	if strings.TrimSpace(baseURL) == "" {
		baseURL = DefaultOpenRouterBaseURL
	}
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}
	return &OpenRouter{
		baseURL:         strings.TrimRight(baseURL, "/"),
		provisioningKey: provisioningKey,
		client:          client,
	}, nil
}

func (o *OpenRouter) Provision(ctx context.Context, taskID string, limit float64) (KeyGrant, error) {
	if taskID == "" {
		return KeyGrant{}, errors.New("task_id is required")
	}
	if limit <= 0 {
		return KeyGrant{}, errors.New("limit must be positive")
	}

	payload, err := json.Marshal(map[string]any{
		"name":  "medios-" + taskID,
		"limit": limit,
	})
	if err != nil {
		return KeyGrant{}, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, o.baseURL+"/api/v1/keys", bytes.NewReader(payload))
	if err != nil {
		return KeyGrant{}, err
	}
	req.Header.Set("Authorization", "Bearer "+o.provisioningKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := o.client.Do(req)
	if err != nil {
		return KeyGrant{}, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return KeyGrant{}, fmt.Errorf("openrouter POST /api/v1/keys returned HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}

	var parsed struct {
		Key  string `json:"key"`
		Data struct {
			Hash  string  `json:"hash"`
			Limit float64 `json:"limit"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return KeyGrant{}, fmt.Errorf("decode openrouter create-key response: %w", err)
	}
	if parsed.Data.Hash == "" || parsed.Key == "" {
		return KeyGrant{}, errors.New("openrouter create-key response missing hash or key")
	}
	lim := parsed.Data.Limit
	if lim == 0 {
		lim = limit
	}
	return KeyGrant{
		Hash:   parsed.Data.Hash,
		Secret: parsed.Key,
		Limit:  lim,
	}, nil
}

func (o *OpenRouter) SettleAndRevoke(ctx context.Context, taskID string, keyHash string) (float64, error) {
	if keyHash == "" {
		return 0, errors.New("key_hash is required")
	}
	keyPath := o.baseURL + "/api/v1/keys/" + url.PathEscape(keyHash)

	// 1. Read exact settled usage before deleting the key.
	getReq, err := http.NewRequestWithContext(ctx, http.MethodGet, keyPath, nil)
	if err != nil {
		return 0, err
	}
	getReq.Header.Set("Authorization", "Bearer "+o.provisioningKey)
	getReq.Header.Set("Accept", "application/json")

	getResp, err := o.client.Do(getReq)
	if err != nil {
		return 0, err
	}
	getRaw, _ := io.ReadAll(getResp.Body)
	_ = getResp.Body.Close()
	if getResp.StatusCode < 200 || getResp.StatusCode >= 300 {
		return 0, fmt.Errorf("openrouter GET key %s returned HTTP %d: %s", keyHash, getResp.StatusCode, strings.TrimSpace(string(getRaw)))
	}

	var meta struct {
		Data struct {
			Usage float64 `json:"usage"`
		} `json:"data"`
	}
	if err := json.Unmarshal(getRaw, &meta); err != nil {
		return 0, fmt.Errorf("decode openrouter get-key response: %w", err)
	}
	if meta.Data.Usage < 0 {
		return 0, fmt.Errorf("negative openrouter usage %v for task %s", meta.Data.Usage, taskID)
	}

	// 2. Revoke the key so it can never be reused after task termination.
	delReq, err := http.NewRequestWithContext(ctx, http.MethodDelete, keyPath, nil)
	if err != nil {
		return 0, err
	}
	delReq.Header.Set("Authorization", "Bearer "+o.provisioningKey)

	delResp, err := o.client.Do(delReq)
	if err != nil {
		return 0, err
	}
	delRaw, _ := io.ReadAll(delResp.Body)
	_ = delResp.Body.Close()
	if delResp.StatusCode != http.StatusNotFound && (delResp.StatusCode < 200 || delResp.StatusCode >= 300) {
		return 0, fmt.Errorf("openrouter DELETE key %s returned HTTP %d: %s", keyHash, delResp.StatusCode, strings.TrimSpace(string(delRaw)))
	}

	return meta.Data.Usage, nil
}
