#!/usr/bin/env bash
# Helpers for verify.sh. Everything talks to the running compose stack from the host.
PIPELINE_URL="${PIPELINE_URL:-http://localhost:3000}"
CONSUMER_URL="${CONSUMER_URL:-http://localhost:3001}"
ES_URL="${ES_URL:-http://localhost:9200}"
UI_URL="${UI_URL:-http://localhost:4200}"
ES_INDEX="${ES_INDEX:-products}"

api()      { local m="$1" p="$2" d="${3:-}"; curl -sf --max-time 900 -X "$m" "$PIPELINE_URL$p" -H 'content-type: application/json' ${d:+-d "$d"}; }
status()   { curl -sf --max-time 10 "$PIPELINE_URL/api/status" | jq -r "$1"; }
metric()   { # metric <name> [label substring]   e.g. metric sink_up 'sink="es"'
  local name="$1" label="${2:-}"
  curl -sf --max-time 10 "$PIPELINE_URL/metrics" | grep -E "^${name}(\{[^}]*${label}[^}]*\})? " | head -1 | awk '{print $2}' | sed 's/^$/0/'
}
cmetric()  { curl -sf --max-time 10 "$CONSUMER_URL/metrics" | grep -E "^$1(\{[^}]*\})? " | head -1 | awk '{print $2}' | sed 's/^$/0/'; }
psql_()    { docker compose exec -T postgres psql -U app -d replication -tA -c "$1"; }
es_refresh() { curl -sf --max-time 30 -X POST "$ES_URL/$ES_INDEX/_refresh" >/dev/null; }
es_count() { es_refresh; curl -sf --max-time 30 "$ES_URL/$ES_INDEX/_count" | jq -r .count; }
# sum of the `version` field over all docs (optionally with a range on id) — equal sums with equal counts
# means every indexed document carries exactly the source's current version (nothing lost, nothing stale).
es_version_sum() {
  local from="${1:-}" to="${2:-}" q='{"match_all":{}}'
  [[ -n "$from" ]] && q="{\"range\":{\"id\":{\"gte\":$from,\"lte\":$to}}}"
  es_refresh
  curl -sf --max-time 60 "$ES_URL/$ES_INDEX/_search" -H 'content-type: application/json' \
    -d "{\"size\":0,\"query\":$q,\"aggs\":{\"v\":{\"sum\":{\"field\":\"version\"}}}}" | jq -r '.aggregations.v.value | floor'
}
src_version_sum() { local w="${1:-true}"; psql_ "select coalesce(sum(version),0) from products where $w"; }
now_ms()   { perl -MTime::HiRes=time -e 'printf "%d\n", time*1000'; }
fmt()      { echo "$1" | perl -pe '1 while s/^(-?\d+)(\d{3})/$1,$2/'; }
container() { docker compose ps -q "$1"; }
pipeline_running() { [[ "$(docker inspect -f '{{.State.Running}}' "$(container pipeline)" 2>/dev/null)" == "true" ]]; }

# wait_until "<shell condition>" <timeout_s> [interval_s]
wait_until() {
  local cond="$1" timeout="$2" interval="${3:-0.5}" start; start=$(date +%s)
  until eval "$cond" >/dev/null 2>&1; do
    if (( $(date +%s) - start >= timeout )); then return 1; fi
    sleep "$interval"
  done
}
wait_healthy() { wait_until "curl -sf $PIPELINE_URL/health" "${1:-120}" 1; }

# ---- report ------------------------------------------------------------------------------
RESULTS=()
FAILED=0
report() { # report "<label>" PASS|FAIL "<detail>"
  local label="$1" verdict="$2" detail="$3" dots
  dots=$(printf '%*s' $((34 - ${#label})) '' | tr ' ' '.')
  RESULTS+=("$label $dots $verdict ($detail)")
  [[ "$verdict" == "FAIL" ]] && FAILED=1
  printf '\n%s %s %s (%s)\n\n' "$label" "$dots" "$verdict" "$detail"
}
say()  { printf '   %s\n' "$*"; }
step() { printf '\n== %s\n' "$*"; }
