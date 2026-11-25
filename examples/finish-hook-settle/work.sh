#!/usr/bin/env bash
# Example work command that records actual token/USD spend into ./usage.json.
# Supports offline fake mode (--fake-spend <amount> [--fail]) or a live
# OpenRouter chat completion (requires OPENROUTER_API_KEY).
set -euo pipefail

FAKE_SPEND=""
FORCE_FAIL=0
MODEL="${OPENROUTER_MODEL:-openai/gpt-4o-mini}"
PROMPT="${PROMPT:-Say hello in five words.}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --fake-spend)
      FAKE_SPEND="$2"
      shift 2
      ;;
    --fail)
      FORCE_FAIL=1
      shift
      ;;
    --model)
      MODEL="$2"
      shift 2
      ;;
    --prompt)
      PROMPT="$2"
      shift 2
      ;;
    *)
      echo "unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

if [[ -n "$FAKE_SPEND" ]]; then
  printf '{"actual": %s, "source": "fake"}\n' "$FAKE_SPEND" > ./usage.json
  echo "fake work complete; wrote usage.json (actual=${FAKE_SPEND})"
  if [[ "$FORCE_FAIL" -eq 1 ]]; then
    echo "simulating non-zero exit after partial spend" >&2
    exit 1
  fi
  exit 0
fi

if [[ -z "${OPENROUTER_API_KEY:-}" ]]; then
  echo "Set OPENROUTER_API_KEY or pass --fake-spend <amount>" >&2
  exit 1
fi

RESP="$(curl -sfS https://openrouter.ai/api/v1/chat/completions \
  -H "Authorization: Bearer ${OPENROUTER_API_KEY}" \
  -H "Content-Type: application/json" \
  -d "$(python3 -c 'import json,sys; print(json.dumps({"model":sys.argv[1],"messages":[{"role":"user","content":sys.argv[2]}]}))' "$MODEL" "$PROMPT")")"

# Extract generation ID and query OpenRouter's settled cost endpoint.
python3 - "$RESP" "$OPENROUTER_API_KEY" <<'PY'
import json, sys, urllib.request

resp = json.loads(sys.argv[1])
api_key = sys.argv[2]
gen_id = resp.get("id", "")
actual = None

if gen_id:
    req = urllib.request.Request(
        f"https://openrouter.ai/api/v1/generation?id={gen_id}",
        headers={"Authorization": f"Bearer {api_key}"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            gen = json.loads(r.read().decode("utf-8"))
            cost = gen.get("data", {}).get("total_cost")
            if cost is not None:
                actual = float(cost)
    except Exception:
        pass

if actual is None:
    usage = resp.get("usage", {})
    actual = float(usage.get("total_tokens", 0))

with open("./usage.json", "w", encoding="utf-8") as f:
    json.dump({"actual": actual, "generation_id": gen_id, "source": "openrouter"}, f)
    f.write("\n")

print(f"openrouter complete; generation_id={gen_id} actual={actual}")
PY
