// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/medios-lab/medios-tokens/internal/ledger"
	"github.com/medios-lab/medios-tokens/internal/provider"
	"github.com/medios-lab/medios-tokens/internal/provisioner"
)

func main() {
	controllerURL := flag.String("controller", "http://127.0.0.1:5050", "Medios controller HTTP base URL")
	nodeURL := flag.String("node", "http://127.0.0.1:5051", "Medios node HTTP base URL")
	resourceName := flag.String("resource", "token_budget", "Scalar resource name to advertise")
	providerName := flag.String("name", "medios-tokens", "Resource provider name")
	initial := flag.Float64("initial", 100.0, "Initial budget balance when no state file exists")
	capVal := flag.Float64("cap", 100.0, "Maximum budget balance after refill or settlement")
	refillAmount := flag.Float64("refill-amount", 40.0, "Amount added on each refill trigger (SIGUSR1, timer, or /refill)")
	refillInterval := flag.Duration("refill-interval", 0, "Periodic refill interval (0 disables timer; use SIGUSR1 or /refill)")
	stateFile := flag.String("state-file", "", "Optional JSON ledger checkpoint path for restart durability")
	adminAddr := flag.String("admin-addr", "", "Optional local HTTP listen address for /balance, /settle, /refill, and /provision (e.g. 127.0.0.1:9095)")
	openRouterKeyEnv := flag.String("openrouter-provisioning-key-env", "OPENROUTER_PROVISIONING_KEY", "Environment variable holding an OpenRouter provisioning key for per-task scoped key minting and terminal settlement")
	openRouterBaseURL := flag.String("openrouter-base-url", provisioner.DefaultOpenRouterBaseURL, "OpenRouter API base URL for key provisioning")
	flag.Parse()

	logger := log.New(os.Stdout, "[medios-tokens] ", log.LstdFlags)

	l, err := ledger.Open(*stateFile, *resourceName, *initial, *capVal)
	if err != nil {
		logger.Fatalf("failed to open ledger: %v", err)
	}

	var prov provisioner.Provisioner
	if *openRouterKeyEnv != "" {
		if rawKey := os.Getenv(*openRouterKeyEnv); rawKey != "" {
			orProv, err := provisioner.NewOpenRouter(rawKey, *openRouterBaseURL, nil)
			if err != nil {
				logger.Fatalf("failed to initialize OpenRouter provisioner: %v", err)
			}
			prov = orProv
			logger.Printf("OpenRouter per-task key provisioning enabled via $%s", *openRouterKeyEnv)
		}
	}

	p := provider.New(provider.Config{
		ControllerURL:  *controllerURL,
		NodeURL:        *nodeURL,
		ProviderName:   *providerName,
		ResourceName:   *resourceName,
		RefillAmount:   *refillAmount,
		RefillInterval: *refillInterval,
		AdminAddr:      *adminAddr,
		Provisioner:    prov,
	}, l, logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if *adminAddr != "" {
		srv := &http.Server{Addr: *adminAddr, Handler: p.AdminHandler()}
		go func() {
			logger.Printf("admin HTTP listening on http://%s (/balance, /settle, /refill, /provision)", *adminAddr)
			if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				logger.Printf("admin HTTP error: %v", err)
			}
		}()
		defer func() {
			shutCtx, c := context.WithTimeout(context.Background(), 2*time.Second)
			defer c()
			_ = srv.Shutdown(shutCtx)
		}()
	}

	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGUSR1)
	go func() {
		for sig := range sigCh {
			if sig == syscall.SIGUSR1 {
				if _, err := p.Refill(ctx, *refillAmount, "SIGUSR1"); err != nil {
					logger.Printf("SIGUSR1 refill error: %v", err)
				}
				continue
			}
			logger.Printf("received %s, shutting down", sig)
			cancel()
			return
		}
	}()

	if err := p.Run(ctx); err != nil {
		logger.Fatalf("provider stopped with error: %v", err)
	}
}
