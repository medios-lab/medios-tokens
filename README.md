# medios-tokens

Reference HTTP Resource Provider for consumable API token budgets on [Medios](https://github.com/medios-lab/medios).

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

## License

Apache License, Version 2.0. See [LICENSE](LICENSE).
