#!/usr/bin/env bash
# make verify — runs the five gates against the running compose stack and prints PASS/FAIL.
# Every gate performs the real failure (docker kill / docker stop / corrupted rows) and asserts
# on the sinks, the checkpoint table and the metrics — never on what the code "should" do.
set -uo pipefail
cd "$(dirname "$0")"
source verify/lib.sh

SEED_ROWS="${SEED_ROWS:-1000000}"
G3_OUTAGE_S="${G3_OUTAGE_S:-60}"
G3_CPU_MAX="${G3_CPU_MAX:-15}"         # % of one core, averaged over the outage
G3_RETRY_RATE_MAX="${G3_RETRY_RATE_MAX:-1}"   # retries per second during the outage
T_START=$(date +%s)
mkdir -p verify/out
exec > >(tee verify/out/verify-$(date +%Y%m%d-%H%M%S).log) 2>&1

step "stack"
docker compose up -d >/dev/null 2>&1 || true          # make sure nothing is left stopped from a previous run
wait_healthy 180 || { echo "pipeline not healthy"; exit 1; }
wait_until "curl -sf $CONSUMER_URL/health" 60 1 || { echo "consumer not healthy"; exit 1; }
say "pipeline, consumer, postgres, elasticsearch, rabbitmq are up"

step "seed $SEED_ROWS rows (clean state: sinks, checkpoints, DLQ, consumer store)"
t0=$(now_ms); api POST /api/admin/seed "{\"rows\": $SEED_ROWS}" >/dev/null || { echo "seed failed"; exit 1; }
SOURCE=$(psql_ "select count(*) from products")
say "seeded $(fmt "$SOURCE") rows in $(( ($(now_ms) - t0) / 1000 ))s"

# =============================================================================================
# G1 crash recovery + G2 no duplicates — one backfill, killed three times
# =============================================================================================
step "G1/G2: backfill with docker kill at 30% and 60%, crash-after-ack at 80%"
g1_ok=1; g1_detail=""
api POST /api/control/backfill/start >/dev/null
TARGET=$(status .backfill.targetMaxId)
kill_and_resume() { # kill_and_resume <pct> <mode: kill|crash>
  local pct=$1 mode=$2 threshold x es_after y replayed t
  threshold=$(( TARGET * pct / 100 ))
  wait_until "[[ \$(status .backfill.lastId) -ge $threshold ]]" 600 0.2 || { say "backfill never reached $pct%"; g1_ok=0; return; }
  if [[ $mode == kill ]]; then
    docker compose kill -s SIGKILL pipeline >/dev/null 2>&1
  else
    api POST /api/sim/crash-after-next-batch >/dev/null
    wait_until "! pipeline_running" 60 0.2 || { say "pipeline did not exit after crash request"; g1_ok=0; return; }
  fi
  x=$(psql_ "select last_id from pipeline_state where key='backfill'")     # durable checkpoint at the moment of death
  es_after=$(es_count)
  replayed=$(( es_after - x )); (( replayed < 0 )) && replayed=0
  t=$(now_ms)
  docker compose start pipeline >/dev/null 2>&1
  wait_healthy 120 || { say "pipeline did not come back"; g1_ok=0; return; }
  y=$(metric backfill_resume_from_id)
  say "$mode at $pct%: checkpoint=$(fmt "$x"), es had $(fmt "$es_after") (window of $replayed beyond checkpoint will be replayed), resumed from $(fmt "$y") in $(( ($(now_ms) - t) / 1000 ))s"
  if [[ "$y" != "$x" || "$y" -le 0 ]]; then say "!! resumed from $y, expected $x"; g1_ok=0; fi
  g1_detail+="killed at $(fmt "$x") / resumed at $(fmt "$y"); "
}
kill_and_resume 30 kill
kill_and_resume 60 kill
kill_and_resume 80 crash
wait_until "[[ \$(status .backfill.status) == done ]]" 900 1 || { say "backfill did not finish"; g1_ok=0; }
say "backfill done; waiting for the consumer to drain the queue"
wait_until "[[ \$(status .counts.queue_depth) == 0 ]]" 600 1 || say "queue did not drain"
sleep 2
ES_COUNT=$(es_count)
CONSUMER_ROWS=$(psql_ "select count(*) from consumed_events")
CONSUMER_DISTINCT=$(psql_ "select count(distinct product_id) from consumed_events")
CONSUMER_DISTINCT_EVENTS=$(psql_ "select count(distinct event_id) from consumed_events")
SRC_SUM=$(src_version_sum); ES_SUM=$(es_version_sum)
LOST=$(( SOURCE - ES_COUNT ))
say "source=$(fmt "$SOURCE") es=$(fmt "$ES_COUNT") consumer rows=$(fmt "$CONSUMER_ROWS") distinct products=$(fmt "$CONSUMER_DISTINCT")"
say "version sums: source=$SRC_SUM es=$ES_SUM (equal ⇒ every doc is at the source's version)"
grep -q '"event":"backfill_resumed"' <(docker compose logs --no-log-prefix pipeline 2>/dev/null) || { say "!! no backfill_resumed log line"; g1_ok=0; }
[[ "$LOST" -eq 0 && "$SRC_SUM" == "$ES_SUM" ]] || g1_ok=0
if (( g1_ok )); then report "G1 resume after kill" PASS "${g1_detail}0 lost"; else report "G1 resume after kill" FAIL "${g1_detail}lost=$LOST"; fi

ES_DUPES_ABSORBED=$(metric duplicates_absorbed_total 'sink="es"' | cut -d. -f1)
C_DUPES_ABSORBED=$(cmetric duplicates_absorbed_total | cut -d. -f1)
DUPES=$(( CONSUMER_ROWS - CONSUMER_DISTINCT_EVENTS ))
g2_ok=1
[[ "$ES_COUNT" == "$SOURCE" && "$CONSUMER_ROWS" == "$SOURCE" && "$CONSUMER_DISTINCT" == "$SOURCE" && "$DUPES" -eq 0 ]] || g2_ok=0
(( ES_DUPES_ABSORBED > 0 && C_DUPES_ABSORBED > 0 )) || { say "!! expected replays to have been absorbed on both sinks (es=$ES_DUPES_ABSORBED consumer=$C_DUPES_ABSORBED)"; g2_ok=0; }
g2_detail="$(fmt "$SOURCE") source / $(fmt "$ES_COUNT") es / $(fmt "$CONSUMER_ROWS") consumer / $DUPES dupes; replays absorbed: es $(fmt "$ES_DUPES_ABSORBED"), consumer $(fmt "$C_DUPES_ABSORBED")"
if (( g2_ok )); then report "G2 no duplicates" PASS "$g2_detail"; else report "G2 no duplicates" FAIL "$g2_detail"; fi

# =============================================================================================
# G3 sink outage — Elasticsearch stopped for 60s while the source keeps changing
# =============================================================================================
step "G3: stop elasticsearch for ${G3_OUTAGE_S}s under incremental load"
g3_ok=1
( end=$(( $(date +%s) + G3_OUTAGE_S + 25 )); while (( $(date +%s) < end )); do api POST /api/sim/generate-changes '{"count":300,"ops":["update","insert","delete"]}' >/dev/null 2>&1; sleep 1; done ) &
GEN_PID=$!
sleep 3
RETRIES_0=$(metric sink_retry_total 'sink="es"' | cut -d. -f1)
docker compose stop -t 5 elasticsearch >/dev/null 2>&1
T_DOWN=$(now_ms)
say "elasticsearch stopped; sampling pipeline CPU during the outage"
cpu_samples=(); PID_C=$(container pipeline)
for i in $(seq 1 5); do
  sleep $(( G3_OUTAGE_S / 5 - 2 ))
  c=$(docker stats --no-stream --format '{{.CPUPerc}}' "$PID_C" | tr -d '%'); cpu_samples+=("$c"); say "cpu sample $i: ${c}%  sink_up{es}=$(metric sink_up 'sink="es"')  state=$(metric pipeline_state)"
done
RETRIES_1=$(metric sink_retry_total 'sink="es"' | cut -d. -f1)
CPU_AVG=$(printf '%s\n' "${cpu_samples[@]}" | awk '{s+=$1} END {printf "%.1f", s/NR}')
RETRY_RATE=$(awk -v a="$RETRIES_0" -v b="$RETRIES_1" -v s="$G3_OUTAGE_S" 'BEGIN {printf "%.2f", (b-a)/s}')
[[ "$(metric sink_up 'sink="es"')" == "0" ]] || { say "!! sink_up{es} should be 0 during the outage"; g3_ok=0; }
docker compose start elasticsearch >/dev/null 2>&1
wait_until "curl -sf '$ES_URL/_cluster/health?wait_for_status=yellow&timeout=1s'" 180 0.5 || { say "es did not come back"; g3_ok=0; }
T_ES_UP=$(now_ms)
wait_until "[[ \$(metric sink_up 'sink=\"es\"') == 1 ]]" 120 0.2 || { say "!! sink_up{es} never returned to 1"; g3_ok=0; }
T_SINK_UP=$(now_ms)
RECOVERY=$(awk -v a="$T_ES_UP" -v b="$T_SINK_UP" 'BEGIN {printf "%.1f", (b-a)/1000}')
wait "$GEN_PID" 2>/dev/null
sleep 4                                   # safety window + one interval
wait_until "[[ \$(metric incremental_lag_seq) == 0 ]]" 300 1 || { say "!! incremental lag did not drain"; g3_ok=0; }
sleep 2
SRC_SUM=$(src_version_sum); ES_SUM=$(es_version_sum); SRC_N=$(psql_ "select count(*) from products"); ES_N=$(es_count)
CHANGED=$(psql_ "select count(*) from change_log")
say "changes generated during/around the outage: $(fmt "$CHANGED"); source rows=$(fmt "$SRC_N") es docs=$(fmt "$ES_N"); version sums source=$SRC_SUM es=$ES_SUM"
say "cpu avg ${CPU_AVG}% (max ${G3_CPU_MAX}%), retries $(( RETRIES_1 - RETRIES_0 )) in ${G3_OUTAGE_S}s = ${RETRY_RATE}/s (max ${G3_RETRY_RATE_MAX}/s), recovered ${RECOVERY}s after ES was reachable"
G3_LOST=$(( SRC_N - ES_N ))
[[ "$SRC_SUM" == "$ES_SUM" && "$G3_LOST" -eq 0 ]] || g3_ok=0
awk -v c="$CPU_AVG" -v m="$G3_CPU_MAX" 'BEGIN {exit !(c < m)}' || { say "!! cpu too high"; g3_ok=0; }
awk -v r="$RETRY_RATE" -v m="$G3_RETRY_RATE_MAX" 'BEGIN {exit !(r < m)}' || { say "!! retry rate too high"; g3_ok=0; }
g3_detail="${G3_OUTAGE_S}s down, $G3_LOST lost, recovered in ${RECOVERY}s, cpu ${CPU_AVG}%, ${RETRY_RATE} retries/s"
if (( g3_ok )); then report "G3 sink outage" PASS "$g3_detail"; else report "G3 sink outage" FAIL "$g3_detail"; fi

# =============================================================================================
# G4 partial batch failure — 3 of 500 rows rejected by the index
# =============================================================================================
step "G4: corrupt 3 rows, touch 500, expect 497 indexed + 3 in DLQ, then fix + replay"
g4_ok=1
BAD_IDS='[1,2,3]'; GOOD_IDS=$(seq -s, 4 500)
api POST /api/control/incremental/pause >/dev/null
api POST /api/sim/corrupt "{\"ids\":$BAD_IDS}" >/dev/null
api POST /api/sim/generate-changes "{\"count\":497,\"ids\":[$GOOD_IDS]}" >/dev/null
api POST /api/control/incremental/resume >/dev/null
sleep 3
wait_until "[[ \$(metric incremental_lag_seq) == 0 ]]" 120 0.5 || { say "!! lag did not drain"; g4_ok=0; }
sleep 1
DLQ_N=$(psql_ "select count(*) from dlq where sink='es' and status='open'")
DLQ_GAUGE=$(metric dlq_size 'sink="es"')
DLQ_CTX=$(psql_ "select count(*) from dlq where sink='es' and status='open' and record_id in (1,2,3) and payload is not null and error like '%parsing_exception%' and batch_id <> ''")
GOOD_SRC=$(src_version_sum "id between 4 and 500"); GOOD_ES=$(es_version_sum 4 500)
WRITTEN=$(psql_ "select count(*) from products where id between 4 and 500")
BAD_ES_STALE=$(curl -sf "$ES_URL/$ES_INDEX/_search" -H 'content-type: application/json' -d '{"size":0,"query":{"terms":{"id":[1,2,3]}},"aggs":{"v":{"max":{"field":"version"}}}}' | jq -r '.aggregations.v.value')
say "DLQ open=$DLQ_N (gauge $DLQ_GAUGE), with full context (payload+error+batch_id)=$DLQ_CTX; good rows version sum source=$GOOD_SRC es=$GOOD_ES; bad rows still at version $BAD_ES_STALE in es"
[[ "$DLQ_N" == 3 && "$DLQ_GAUGE" == 3 && "$DLQ_CTX" == 3 && "$GOOD_SRC" == "$GOOD_ES" ]] || g4_ok=0
say "sample DLQ entry:"; psql_ "select record_id, version, attempts, mode, left(error, 110) from dlq where status='open' order by record_id limit 1" | sed 's/^/     /'
# fix at the source, replay from the DLQ (incremental paused so the replay path is what re-indexes them)
api POST /api/control/incremental/pause >/dev/null
api POST /api/sim/fix "{\"ids\":$BAD_IDS}" >/dev/null
REPLAY=$(api POST /api/dlq/replay '{"all":true}')
say "replay: $REPLAY"
DLQ_AFTER=$(psql_ "select count(*) from dlq where sink='es' and status='open'")
BAD_SRC=$(src_version_sum "id in (1,2,3)"); BAD_ES=$(es_version_sum 1 3)
api POST /api/control/incremental/resume >/dev/null
say "after replay: DLQ open=$DLQ_AFTER; bad rows version sum source=$BAD_SRC es=$BAD_ES"
[[ "$DLQ_AFTER" == 0 && "$BAD_SRC" == "$BAD_ES" && "$(echo "$REPLAY" | jq -r .resolved)" == 3 ]] || g4_ok=0
g4_detail="$WRITTEN written, $DLQ_N in DLQ, $(echo "$REPLAY" | jq -r .resolved) replayed after fix"
if (( g4_ok )); then report "G4 partial batch failure" PASS "$g4_detail"; else report "G4 partial batch failure" FAIL "$g4_detail"; fi

# =============================================================================================
# G5 observability — the questions must be answerable from /metrics, /health, logs and the UI
# =============================================================================================
step "G5: metrics, health, lag semantics, logs, UI"
g5_ok=1; g5_notes=()
M=$(curl -sf "$PIPELINE_URL/metrics")
for name in backfill_position backfill_target backfill_status backfill_resume_from_id records_written_total records_per_second incremental_cursor incremental_lag_seq incremental_lag_seconds dlq_size sink_up sink_retry_total duplicates_absorbed_total pipeline_state; do
  grep -qE "^${name}(\{|\s)" <<<"$M" || { say "!! metric missing: $name"; g5_ok=0; }
done
CM=$(curl -sf "$CONSUMER_URL/metrics")
for name in messages_received_total events_stored_total duplicates_absorbed_total consumer_dlq_size consumer_up; do
  grep -qE "^${name}(\{|\s)" <<<"$CM" || { say "!! consumer metric missing: $name"; g5_ok=0; }
done
H=$(curl -sf "$PIPELINE_URL/health"); say "health: $H"
[[ "$(jq -r .status <<<"$H")" == ok && "$(jq -r .sinks.es <<<"$H")" == true && "$(jq -r .sinks.rabbitmq <<<"$H")" == true ]] || { say "!! health not ok"; g5_ok=0; }
POS=$(metric backfill_position); CK=$(psql_ "select last_id from pipeline_state where key='backfill'")
[[ "$POS" == "$CK" ]] && say "where is the backfill? backfill_position=$(fmt "$POS") == checkpoint table $(fmt "$CK"), backfill_status=$(metric backfill_status) (3=done)" || { say "!! backfill_position $POS != checkpoint $CK"; g5_ok=0; }
api POST /api/control/incremental/pause >/dev/null
api POST /api/sim/generate-changes '{"count":1000}' >/dev/null
sleep 4
LAG=$(metric incremental_lag_seq); LAG_S=$(metric incremental_lag_seconds)
say "incremental paused + 1,000 changes: incremental_lag_seq=$LAG incremental_lag_seconds=$LAG_S"
(( LAG >= 1000 )) || { say "!! lag metric did not rise"; g5_ok=0; }
api POST /api/control/incremental/resume >/dev/null
wait_until "[[ \$(metric incremental_lag_seq) == 0 ]]" 120 0.5 && say "resumed: lag drained to 0, records_per_second{incremental}=$(metric records_per_second 'mode="incremental"')" || { say "!! lag did not drain"; g5_ok=0; }
say "DLQ from metrics: dlq_size{es}=$(metric dlq_size 'sink="es"'); throughput seen during backfill is in the log (records_per_second{backfill})"
LOGS=$(docker compose logs --no-log-prefix pipeline 2>/dev/null)
for ev in backfill_started backfill_resumed backfill_done sink_down sink_up dlq_item crash_requested; do
  n=$(grep -c "\"event\":\"$ev\"" <<<"$LOGS"); (( n > 0 )) && say "logs: $ev x$n" || { say "!! log event missing: $ev"; g5_ok=0; }
done
UI_CODE=$(curl -s -o /dev/null -w '%{http_code}' "$UI_URL/")
[[ "$UI_CODE" == 200 ]] && say "ui: $UI_URL -> 200" || { say "!! ui returned $UI_CODE"; g5_ok=0; }
if (( g5_ok )); then report "G5 observability" PASS "metrics+health+logs+ui answer position/throughput/lag/DLQ/health"; else report "G5 observability" FAIL "see notes above"; fi

# =============================================================================================
printf '\n================ verify report (%ss, %s rows) ================\n' "$(( $(date +%s) - T_START ))" "$(fmt "$SOURCE")"
printf '%s\n' "${RESULTS[@]}" | tee verify/out/report.txt
printf '===============================================================\n'
exit $FAILED
