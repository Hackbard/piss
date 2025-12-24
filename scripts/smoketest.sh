#!/usr/bin/env bash
set -Eeuo pipefail

# ---------------------------------
# Hard defaults (as requested)
# ---------------------------------
MEILI_URL="${MEILI_URL:-http://localhost:7700}"
MEILI_MASTER_KEY="masterKey"   # <-- fixed as requested

NEO4J_USER="${NEO4J_USER:-neo4j}"
NEO4J_PASSWORD="${NEO4J_PASSWORD:-password}"

SMOKE_PERSON_QUERY="${SMOKE_PERSON_QUERY:-Stephan Weil}"

REPORT_DIR="${REPORT_DIR:-smoketest_reports}"
mkdir -p "$REPORT_DIR"
TS_UTC="$(date -u +"%Y%m%d_%H%M%S")"
GIT_SHA="$(git rev-parse --short HEAD 2>/dev/null || echo "nogit")"
REPORT_FILE="${REPORT_DIR}/smoketest_${TS_UTC}_${GIT_SHA}.txt"

FAILS=0

log() { echo "$*" | tee -a "$REPORT_FILE"; }

section() {
  log ""
  log "============================================================"
  log "$1"
  log "============================================================"
}

run_cmd() {
  local title="$1"; shift
  section "$title"
  log "TIME: $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  log "CMD: $*"
  log "------------------------------------------------------------"

  set +e
  "$@" 2>&1 | tee -a "$REPORT_FILE"
  local rc=${PIPESTATUS[0]}
  set -e

  log ""
  log "EXIT_CODE: $rc"

  if [[ $rc -ne 0 ]]; then
    FAILS=$((FAILS + 1))
    log "NOTE: Command failed; continuing. (FAILS=$FAILS)"
  fi
}

wait_http() {
  local url="$1" name="$2" max_secs="${3:-60}"
  local start; start="$(date +%s)"
  while true; do
    if curl -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
    local now; now="$(date +%s)"
    if (( now - start >= max_secs )); then
      log "TIMEOUT waiting for $name at $url after ${max_secs}s"
      return 1
    fi
  done
}

dc() { docker compose "$@"; }

# Preconditions
command -v docker >/dev/null 2>&1 || { echo "docker missing"; exit 2; }
docker compose version >/dev/null 2>&1 || { echo "docker compose missing"; exit 2; }
command -v curl >/dev/null 2>&1 || { echo "curl missing"; exit 2; }
command -v jq >/dev/null 2>&1 || { echo "jq missing (brew install jq)"; exit 2; }

ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
cd "$ROOT"

log "SMOKETEST START: $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
log "PWD: $ROOT"
log "GIT: $GIT_SHA"
log "REPORT: $REPORT_FILE"
log "MEILI_URL: $MEILI_URL"
log "MEILI_MASTER_KEY: (fixed) masterKey"

# ----------------------------
# 1) Docker / services
# ----------------------------
run_cmd "Docker compose ps" dc ps
run_cmd "Bring up services (neo4j + meilisearch)" dc up -d neo4j meilisearch

section "Wait for Meilisearch"
if wait_http "${MEILI_URL}/health" "Meilisearch" 60; then
  log "OK: Meilisearch reachable."
else
  FAILS=$((FAILS + 1))
  log "FAIL: Meilisearch not reachable. (FAILS=$FAILS)"
fi

# ----------------------------
# 2) Meilisearch checks
# ----------------------------
run_cmd "Meilisearch /health" bash -lc "curl -sS '${MEILI_URL}/health' | jq"

run_cmd "Meilisearch list indexes" bash -lc \
  "curl -sS -H 'Authorization: Bearer ${MEILI_MASTER_KEY}' '${MEILI_URL}/indexes' | jq"

run_cmd "Meilisearch persons stats" bash -lc \
  "curl -sS -H 'Authorization: Bearer ${MEILI_MASTER_KEY}' '${MEILI_URL}/indexes/persons/stats' \
   | jq '{numberOfDocuments,isIndexing,fieldDistribution:(.fieldDistribution|keys)}'"

run_cmd "Meilisearch query: ${SMOKE_PERSON_QUERY} doc shape" \
  bash -lc "curl -sS \
    -H 'Authorization: Bearer ${MEILI_MASTER_KEY}' \
    -H 'Content-Type: application/json' \
    --data-binary '{\"q\":\"${SMOKE_PERSON_QUERY}\",\"limit\":1}' \
    '${MEILI_URL}/indexes/persons/search' \
  | jq '.hits[0] | {id,name,evidence_ids,evidence_refs,evidence_snippet_refs}'"

run_cmd "Meilisearch query: Merkel (expected empty unless DIP ingested)" \
  bash -lc "curl -sS \
    -H 'Authorization: Bearer ${MEILI_MASTER_KEY}' \
    -H 'Content-Type: application/json' \
    --data-binary '{\"q\":\"Merkel\",\"limit\":5}' \
    '${MEILI_URL}/indexes/persons/search' \
  | jq '{query,estimatedTotalHits}'"

# ----------------------------
# 3) Neo4j checks
# ----------------------------
run_cmd "Neo4j count CanonicalPerson" \
  dc exec -T neo4j cypher-shell -u "$NEO4J_USER" -p "$NEO4J_PASSWORD" --format plain \
  "MATCH (c:CanonicalPerson) RETURN count(c) AS canonical_count;"

run_cmd "Neo4j count PersonLinkAssertion by status" \
  dc exec -T neo4j cypher-shell -u "$NEO4J_USER" -p "$NEO4J_PASSWORD" --format plain \
  "MATCH (a:PersonLinkAssertion) RETURN a.status AS status, count(a) AS count ORDER BY status;"

run_cmd "Neo4j count Evidence nodes" \
  dc exec -T neo4j cypher-shell -u "$NEO4J_USER" -p "$NEO4J_PASSWORD" --format plain \
  "MATCH (e:Evidence) RETURN count(e) AS evidence_count;"

# ----------------------------
# 4) CLI smoke
# ----------------------------
run_cmd "Scraper CLI help" dc run --rm scraper scraper --help
run_cmd "Scraper seed validate (flag form)" dc run --rm scraper scraper seed --validate
run_cmd "Scraper evidence help" dc run --rm scraper scraper evidence --help

# ----------------------------
# 5) Evidence resolver correctness check
# ----------------------------
section "Evidence resolve-from-meili (prefer table_row) + consistency assertion"
TMP_OUT="$(mktemp)"
set +e
dc run --rm scraper scraper evidence \
  --resolve-from-meili \
  --query "${SMOKE_PERSON_QUERY}" \
  --index persons \
  --limit 1 \
  --prefer table_row \
  --with-snippets \
  --format md 2>&1 | tee -a "$REPORT_FILE" | tee "$TMP_OUT"
RC=${PIPESTATUS[0]}
set -e

log ""
log "EXIT_CODE: $RC"
if [[ $RC -ne 0 ]]; then
  FAILS=$((FAILS + 1))
  log "FAIL: evidence resolver command failed. (FAILS=$FAILS)"
else
  # minimal sanity: for Stephan Weil we expect the snippet-ref to point to Stephan_Weil if binding is correct
  if [[ "${SMOKE_PERSON_QUERY}" == "Stephan Weil" ]]; then
    if grep -q '"person_title": "Stephan_Weil"' "$TMP_OUT"; then
      log "OK: Evidence snippet-ref matches Stephan_Weil"
    else
      FAILS=$((FAILS + 1))
      log "FAIL: Evidence snippet-ref did NOT match Stephan_Weil (row->person mismatch likely). (FAILS=$FAILS)"
    fi
  fi
fi
rm -f "$TMP_OUT"

# ----------------------------
# Summary
# ----------------------------
section "SMOKETEST SUMMARY"
log "FAILS: $FAILS"
log "SMOKETEST END: $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
log "REPORT: $REPORT_FILE"

echo
echo "Done. Report written to:"
echo "  $REPORT_FILE"
echo "Post it with:"
echo "  sed -n '1,260p' $REPORT_FILE"
echo "  sed -n '260,520p' $REPORT_FILE"

exit 0
