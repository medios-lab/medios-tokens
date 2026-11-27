# medios-tokens

> **Status: Research Preview.** `medios-tokens` is an experimental reference implementation demonstrating consumable budget semantics on an unmodified Medios broker. It is scoped to a single provider on a single node for reproducible systems research and is not a production billing service.

Reference HTTP Resource Provider for consumable API token budgets on [Medios](https://github.com/medios-lab/medios).

**Technical Note (PDF):** [Consumable API Token Budgets in an Unmodified Two-Level Resource Broker](papers/brokering-consumable-budgets/main.pdf) ([LaTeX source](papers/brokering-consumable-budgets/main.tex)).

**Organizing principle:** Medios brokers resources it does not need to understand. `medios-gpu` shows you can extend the resource vocabulary with reusable hardware (`acquire -> use -> release`) without modifying Medios core; `medios-tokens` shows you can extend resource lifecycle semantics to consumable budgets (`reserve -> spend -> settle`) with zero core modifications and zero C++ coupling.

## What it does

1. Reads one or more externally governed budgets (`--initial` and `--cap`, a `--config` file of named pools, an HTTP balance endpoint per pool, or a durable `--state-file` checkpoint).
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

## Several pools from one provider

`--config` replaces `--resource`, `--initial`, `--cap` and `--refill-amount` with a JSON file that declares any number of named pools. Each pool is advertised as its own scalar resource in the same `UPDATE_STATE`, and a task that carries several of them is debited from all of them in one ledger update.

```json
{
  "pools": [
    {
      "resource": "tauopt_suggestions",
      "initial": 1000,
      "cap": 1000,
      "sync": {
        "url": "https://api.tauopt.com/v1/me",
        "auth_header": "Authorization",
        "auth_prefix": "Bearer ",
        "auth_env": "TAUOPT_API_TOKEN",
        "interval": "60s",
        "cap_path": "org.caps.org.suggests_per_day",
        "used_path": "org.usage.suggests_24h"
      }
    },
    {
      "resource": "openrouter_usd",
      "initial": 50.0,
      "cap": 50.0,
      "refill_amount": 0,
      "provisioner": "openrouter"
    }
  ]
}
```

```sh
export TAUOPT_API_TOKEN="..."
export OPENROUTER_PROVISIONING_KEY="sk-or-v1-..."
./medios-tokens \
  --controller=http://127.0.0.1:5050 \
  --node=http://127.0.0.1:5051 \
  --config=pools.json \
  --state-file=/tmp/medios-tokens.state.json \
  --admin-addr=127.0.0.1:9095
```

Pool fields:

| Field | Meaning |
| --- | --- |
| `resource` | Scalar resource name advertised to Medios. Required and unique. |
| `initial`, `cap` | Balance and cap the pool starts with when the state file does not already hold it. |
| `refill_amount` | Amount added on `SIGUSR1`, on the `--refill-interval` timer, and on `POST /refill` without an amount. `0` or omitted leaves the pool alone on those triggers. Not allowed together with `sync`. |
| `provisioner` | `"openrouter"` binds per-task key provisioning (below) to this pool. At most one pool may set it, and the provisioning key variable must then be set. |
| `sync` | Makes the pool's balance follow an HTTP JSON endpoint. |

With more than one pool, `POST /settle` and `POST /refill` require a `"resource"` field naming the pool; with one pool the field is optional. `GET /balance` returns every pool under `pools`, and with one pool it also returns that pool's `resource`, `balance`, `cap` and `tasks` at the top level.

A state file written by a single-pool run loads under `--config` when the config declares the same resource name; pools the file does not hold are added from the config. A state file that holds a pool the config does not declare stops startup with an error.

### Balance sync from an HTTP endpoint

A pool with a `sync` block is read once before the provider subscribes and then every `interval`. Each read is one `GET` of `url`; the balance is taken from the JSON response by dot-separated object keys.

| Field | Meaning |
| --- | --- |
| `url` | Absolute `http` or `https` URL. Redirects are not followed. |
| `cap_path`, `used_path` | Paths to the external cap and the amount used; available = cap - used. |
| `remaining_path` | Path to the amount remaining, used instead of `used_path`. `cap_path` is then optional. |
| `auth_header`, `auth_prefix`, `auth_env` | The request carries `<auth_header>: <auth_prefix><value of $auth_env>`. Omit all three for an endpoint without authentication. If `auth_env` is named and the variable is unset or empty, startup fails. |
| `interval` | Time between reads, as a Go duration. Default `60s`. |
| `timeout` | Limit for one read. Default `10s`. |

After each read the pool's cap becomes the external cap and its balance becomes

```
max(0, external available - reservations of tasks not yet settled)
```

so capacity already promised to running tasks is not offered a second time. Until a task settles, its full reservation is subtracted even if the endpoint already counts part of its spend, which makes the advertised balance lower than the true one while the task runs.

The advertised balance can be higher than the true one in two cases. If the endpoint reports a task's spend some time after the task settles, reads in between see the reservation gone and the spend not yet counted. Spend against the same external quota that did not pass through a task reservation is visible only after the next read.

When a read fails (connection error, timeout, a status other than 200, or a response without the configured paths), the provider logs the failure and keeps advertising the last balance. There is no limit on how long it does so. The credential value is never written to the log or the state file.

`POST /refill` on a synced pool returns HTTP 409.

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
