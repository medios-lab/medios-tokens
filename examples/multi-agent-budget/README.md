# Multi-Agent Budget Contention vs. Orchestration

Live side-by-side benchmark running **6 multi-agent workflows** (4 three-step research chains + 2 background digests) against a single `100`-unit `token_budget` pool advertised by `medios-tokens` on an unmodified Medios cluster.

- **Case 1 (Naive FIFO Queue Contention):** Blanket worst-case reservations (`25` per step), FIFO arrival, no chain pre-planning, no model-tier downgrading. `medios-tokens` still enforces the hard cap, but low-value tasks and partial chains fragment the pool and strand all 4 research workflows at step 2 of 3.
- **Case 2 (Smart Budget Orchestrator):** The exact same Medios controller, node, and `medios-tokens` provider—with scheduling policy in the framework:
  - Priority and full-chain admission before starting step 1
  - Tight per-step reservations (`8`, `10`, `9`) with immediate settlement recycling (`POST /settle`)
  - Watermark-based model-tier routing (`<= 25` remaining routes background digests to a cheaper model tier)

## Run

```sh
python3 examples/multi-agent-budget/benchmark.py \
  --medios-build /path/to/medios/build \
  --provider-bin ./medios-tokens
```
