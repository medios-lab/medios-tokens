# medios-tokens

> **Status: Research Preview.** `medios-tokens` is an experimental reference implementation demonstrating consumable budget semantics on an unmodified Medios broker. It is scoped to a single provider on a single node for reproducible systems research and is not a production billing service.

Reference HTTP Resource Provider for consumable API token budgets on [Medios](https://github.com/medios-lab/medios).

**Technical Note (PDF):** [Consumable API Token Budgets in an Unmodified Two-Level Resource Broker](papers/brokering-consumable-budgets/main.pdf) ([LaTeX source](papers/brokering-consumable-budgets/main.tex)).

**Organizing principle:** Medios brokers resources it does not need to understand. `medios-gpu` shows you can extend the resource vocabulary with reusable hardware (`acquire -> use -> release`) without modifying Medios core; `medios-tokens` shows you can extend resource lifecycle semantics to consumable budgets (`reserve -> spend -> settle`) with zero core modifications and zero C++ coupling.

## What it does

1. Reads an externally governed budget (`--initial`, `--cap`, or durable `--state-file` checkpoint).
2. Advertises available capacity to a `medios-node` over `POST /api/v1/resource_provider` (`UPDATE_STATE`).
3. Authorizes task launch reservations (`Event::PUBLISH_RESOURCES` -> `UPDATE_PUBLISH_RESOURCES_STATUS`).
4. Debits each accepted task once per `task_id` via controller `/api/v1` `TASK_ADDED` events, and optionally reconciles actual consumption (`POST /settle`) to release unused capacity back into the pool.
5. Updates Medios whenever the balance changes (`SIGUSR1`, `--refill-interval`, or `POST /refill`).

Zero third-party Go dependencies (Go 1.26 standard library only).

## Build and test

```sh
go test -race ./...
go build -o medios-tokens ./cmd/medios-tokens
```

## Run

Start `medios-tokens` beside a running `medios-controller` and `medios-node`:

```sh
./medios-tokens \
  --controller=http://127.0.0.1:5050 \
  --node=http://127.0.0.1:5051 \
  --resource=token_budget \
  --initial=100 \
  --cap=100 \
  --refill-amount=40 \
  --state-file=/tmp/medios-tokens.state.json \
  --admin-addr=127.0.0.1:9095
```

Frameworks subscribed to `/api/v1/scheduler` immediately receive `token_budget` inside node offers alongside `cpus` and `mem`, and carve it identically to any scalar resource.

## Settle actual spend and refill

Inspect the live ledger:

```sh
curl -s http://127.0.0.1:9095/balance
```

Reconcile a task that reserved `25` tokens and spent `18` (releasing `7` back to Medios offers):

```sh
curl -s -X POST http://127.0.0.1:9095/settle \
  -H 'Content-Type: application/json' \
  -d '{"task_id":"task-1","actual":18}'
```

Replenish the balance externally (or send `kill -USR1 <pid>`):

```sh
curl -s -X POST http://127.0.0.1:9095/refill \
  -H 'Content-Type: application/json' \
  -d '{"amount":40}'
```

## Per-task scoped keys for OpenRouter (Pattern 2)

For stronger enforcement than task-reported settlement, set `OPENROUTER_PROVISIONING_KEY` (or pass `--openrouter-provisioning-key-env`):

```sh
export OPENROUTER_PROVISIONING_KEY="sk-or-v1-..."
./medios-tokens \
  --controller=http://127.0.0.1:5050 \
  --node=http://127.0.0.1:5051 \
  --state-file=/tmp/medios-tokens.state.json \
  --admin-addr=127.0.0.1:9095
```

Before launching a task, a scheduler mints a hard-capped ephemeral key for that `task_id`:

```sh
curl -s -X POST http://127.0.0.1:9095/provision \
  -H 'Content-Type: application/json' \
  -d '{"task_id":"task-1","limit":25}'
```

`medios-tokens` returns the secret key once (for injection into the task environment) and checkpoints only the opaque `key_hash` in its ledger. When the controller emits a terminal `TASK_UPDATED` event (`TASK_FINISHED`, `TASK_FAILED`, `TASK_KILLED`, `TASK_LOST`, etc.), `medios-tokens` automatically reads exact spend from OpenRouter (`GET /api/v1/keys/{hash}`), revokes the key (`DELETE /api/v1/keys/{hash}`), and returns unused credit to Medios offers via `UPDATE_STATE`.

## Worked examples

- [examples/finish-hook-settle](examples/finish-hook-settle/README.md): end-to-end Task Finish Hook settlement (Pattern 1) showing a task writing actual LLM spend to `./usage.json`, a finish hook posting to `POST /settle`, fail-closed semantics on missing usage, and a live smoke test against `medios-controller` and `medios-node`.
- [examples/multi-agent-budget](examples/multi-agent-budget/README.md): live side-by-side benchmark comparing naive FIFO queue contention against a budget-aware multi-agent orchestrator sharing one `100`-unit `token_budget` pool.

## License

Apache License, Version 2.0. See [LICENSE](LICENSE).
