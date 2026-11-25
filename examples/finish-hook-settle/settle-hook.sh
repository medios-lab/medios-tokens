#!/usr/bin/env bash
# Medios Task Finish Hook that reconciles actual spend with medios-tokens /settle.
#
# Injected by medios-executor on v0.1.14+ (cannot be overridden by the task):
#   MEDIOS_TASK_ID          — unique task ID debited by medios-tokens
#   MEDIOS_WORK_EXIT_STATUS — exit status of the work command
#
# Required from environment:
#   MEDIOS_TOKENS_ADMIN_URL — base URL of medios-tokens admin HTTP (e.g. http://127.0.0.1:9095)
#
# Fail-closed semantics: if ./usage.json is absent/malformed or /settle is
# unreachable, this hook exits cleanly or non-zero without refunding anything,
# leaving the full reservation debited in medios-tokens.
set -euo pipefail

ADMIN_URL="${MEDIOS_TOKENS_ADMIN_URL:-http://127.0.0.1:9095}"
TASK_ID="${MEDIOS_TASK_ID:-}"
USAGE_FILE="${USAGE_FILE:-./usage.json}"

if [[ -z "$TASK_ID" ]]; then
  echo "settle-hook: MEDIOS_TASK_ID is unset; leaving full reservation debited" >&2
  exit 1
fi

if [[ ! -f "$USAGE_FILE" ]]; then
  echo "settle-hook: ${USAGE_FILE} not found; leaving full reservation debited" >&2
  exit 0
fi

ACTUAL="$(python3 - "$USAGE_FILE" <<'PY'
import json, sys
data = json.load(open(sys.argv[1], encoding="utf-8"))
val = float(data["actual"])
if val < 0:
    raise ValueError("negative actual spend")
print(val)
PY
)"

PAYLOAD="$(python3 -c 'import json,sys; print(json.dumps({"task_id":sys.argv[1],"actual":float(sys.argv[2])}))' "$TASK_ID" "$ACTUAL")"

RESP="$(curl -sfS -X POST "${ADMIN_URL%/}/settle" \
  -H 'Content-Type: application/json' \
  -d "$PAYLOAD")"

echo "settle-hook: settled task=${TASK_ID} actual=${ACTUAL} response=${RESP}"
