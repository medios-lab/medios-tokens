# Finish-Hook Settlement Example (Pattern 1)

Demonstrates task-level `reserve -> spend -> settle` using a Medios Task Finish Hook (`TaskInfo.finish`) and `medios-tokens` `POST /settle`.

## How it works

1. A framework launches a task reserving `token_budget: 25`. On `TASK_ADDED`, `medios-tokens` debits `25` immediately.
2. The work command ([`work.sh`](work.sh)) calls an LLM API (or runs in `--fake-spend` mode for offline testing) and writes `{"actual": <number>}` to `./usage.json` inside the task sandbox.
3. When the work command exits — cleanly, with a non-zero exit code, or after a kill — `medios-executor` runs the bounded finish hook ([`settle-hook.sh`](settle-hook.sh)) with the non-overridable `MEDIOS_TASK_ID` environment variable.
4. `settle-hook.sh` posts `{"task_id": "$MEDIOS_TASK_ID", "actual": <number>}` to `medios-tokens` `/settle`.
5. `medios-tokens` credits back `reserved - actual` (up to `--cap`), checkpoints the ledger, and emits `UPDATE_STATE` so the unused capacity reappears in Medios offers.

## Failure semantics (fail-closed)

- If `./usage.json` is missing or malformed, `settle-hook.sh` leaves the full reservation debited.
- If `medios-tokens` `/settle` is unreachable or the host crashes before the hook runs, the full reservation stays debited.
- The provider never overspends because reservations are debited up front and unused credit is only returned on explicit settlement.

## Run the offline smoke test

Requires compiled `medios-controller`, `medios-node`, `medios-executor`, and `medios-tokens` binaries:

```sh
python3 examples/finish-hook-settle/smoke.py \
  --medios-build /path/to/medios/build \
  --provider-bin ./medios-tokens
```

Pass `--use-native-finish-hook` when running against `medios v0.1.14+` to submit `TaskInfo.finish` directly; otherwise the smoke script executes the identical `settle-hook.sh` via an `EXIT` trap inside the task sandbox.

## Live OpenRouter usage

Set `OPENROUTER_API_KEY` on the task environment and omit `--fake-spend` in `work.sh`. The script calls `https://openrouter.ai/api/v1/chat/completions`, queries `GET /api/v1/generation?id=<id>` for the exact settled `total_cost`, writes `./usage.json`, and `settle-hook.sh` reconciles the unused reservation against `medios-tokens`.
