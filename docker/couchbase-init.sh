#!/bin/sh
# Idempotent Couchbase cluster + bucket initialisation.
# Runs once (restart: "no") after the couchbase service passes its healthcheck.
# All indexes, scopes, and collections match what the app creates at runtime.
set -e

CB_HOST="${CB_HOST:-couchbase}"
CB_USER="${CB_USER:-Administrator}"
CB_PASS="${CB_PASS:-password}"
CB_BUCKET="${CB_BUCKET:-supportal}"
CB_RAMSIZE="${CB_RAMSIZE:-1024}"            # data service RAM (MB)
CB_INDEX_RAMSIZE="${CB_INDEX_RAMSIZE:-512}" # index service RAM (MB)
CB_FTS_RAMSIZE="${CB_FTS_RAMSIZE:-512}"     # FTS service RAM (MB)
CB_BUCKET_RAMSIZE="${CB_BUCKET_RAMSIZE:-256}"

BASE="http://${CB_HOST}:8091"
N1QL="http://${CB_HOST}:8093/query/service"

log() { printf '[couchbase-init] %s\n' "$*"; }

# ── 1. Wait for REST API ──────────────────────────────────────────────────────
log "Waiting for Couchbase REST API..."
until curl -sf "${BASE}/pools" >/dev/null 2>&1; do
    sleep 2
done
log "REST API ready."

# ── 2. Idempotency check ──────────────────────────────────────────────────────
if curl -sf "${BASE}/pools/default" -u "${CB_USER}:${CB_PASS}" >/dev/null 2>&1; then
    log "Cluster already initialised — skipping."
    exit 0
fi

# ── 3. Memory quotas ──────────────────────────────────────────────────────────
log "Setting memory quotas (data=${CB_RAMSIZE}MB index=${CB_INDEX_RAMSIZE}MB fts=${CB_FTS_RAMSIZE}MB)..."
curl -sf -X POST "${BASE}/pools/default" \
    -d "memoryQuota=${CB_RAMSIZE}" \
    -d "indexMemoryQuota=${CB_INDEX_RAMSIZE}" \
    -d "ftsMemoryQuota=${CB_FTS_RAMSIZE}"

# ── 4. Services ───────────────────────────────────────────────────────────────
log "Enabling services (kv, n1ql, index, fts)..."
curl -sf -X POST "${BASE}/node/controller/setupServices" \
    -d "services=kv%2Cn1ql%2Cindex%2Cfts"

# ── 5. Admin credentials ──────────────────────────────────────────────────────
log "Setting admin credentials..."
curl -sf -X POST "${BASE}/settings/web" \
    -d "username=${CB_USER}" \
    -d "password=${CB_PASS}" \
    -d "port=SAME"
log "Cluster configured."

# ── 6. Bucket ─────────────────────────────────────────────────────────────────
log "Creating bucket '${CB_BUCKET}' (${CB_BUCKET_RAMSIZE} MB)..."
curl -sf -u "${CB_USER}:${CB_PASS}" \
    -X POST "${BASE}/pools/default/buckets" \
    -d "name=${CB_BUCKET}" \
    -d "bucketType=couchbase" \
    -d "ramQuotaMB=${CB_BUCKET_RAMSIZE}" \
    -d "replicaNumber=0"

until curl -sf -u "${CB_USER}:${CB_PASS}" \
    "${BASE}/pools/default/buckets/${CB_BUCKET}" >/dev/null 2>&1; do
    sleep 2
done
log "Bucket ready."

# ── 7. Wait for N1QL ──────────────────────────────────────────────────────────
log "Waiting for Query service..."
until curl -sf -u "${CB_USER}:${CB_PASS}" \
    -X POST "${N1QL}" -d "statement=SELECT+1" >/dev/null 2>&1; do
    sleep 3
done
log "Query service ready."

# ── 8. Scopes ─────────────────────────────────────────────────────────────────
log "Creating scopes..."
SCOPES="${BASE}/pools/default/buckets/${CB_BUCKET}/scopes"

curl -sf -u "${CB_USER}:${CB_PASS}" -X POST "${SCOPES}" -d "name=transcripts"
curl -sf -u "${CB_USER}:${CB_PASS}" -X POST "${SCOPES}" -d "name=chat"

# ── 9. Collections ────────────────────────────────────────────────────────────
log "Creating collections..."

# transcripts scope: ticket data, snapshots, assets, brand kits, observability,
# insights, and the Salesforce mirror (accounts + opportunities).
curl -sf -u "${CB_USER}:${CB_PASS}" -X POST "${SCOPES}/transcripts/collections" -d "name=tickets"
curl -sf -u "${CB_USER}:${CB_PASS}" -X POST "${SCOPES}/transcripts/collections" -d "name=snapshots"
curl -sf -u "${CB_USER}:${CB_PASS}" -X POST "${SCOPES}/transcripts/collections" -d "name=assets"
curl -sf -u "${CB_USER}:${CB_PASS}" -X POST "${SCOPES}/transcripts/collections" -d "name=brands"
curl -sf -u "${CB_USER}:${CB_PASS}" -X POST "${SCOPES}/transcripts/collections" -d "name=markers"
curl -sf -u "${CB_USER}:${CB_PASS}" -X POST "${SCOPES}/transcripts/collections" -d "name=insights"
curl -sf -u "${CB_USER}:${CB_PASS}" -X POST "${SCOPES}/transcripts/collections" -d "name=accounts"
curl -sf -u "${CB_USER}:${CB_PASS}" -X POST "${SCOPES}/transcripts/collections" -d "name=opportunities"

# chat scope: Corax conversation persistence
curl -sf -u "${CB_USER}:${CB_PASS}" -X POST "${SCOPES}/chat/collections" -d "name=history"
curl -sf -u "${CB_USER}:${CB_PASS}" -X POST "${SCOPES}/chat/collections" -d "name=profiles"
curl -sf -u "${CB_USER}:${CB_PASS}" -X POST "${SCOPES}/chat/collections" -d "name=threads"
curl -sf -u "${CB_USER}:${CB_PASS}" -X POST "${SCOPES}/chat/collections" -d "name=steps"
curl -sf -u "${CB_USER}:${CB_PASS}" -X POST "${SCOPES}/chat/collections" -d "name=elements"
curl -sf -u "${CB_USER}:${CB_PASS}" -X POST "${SCOPES}/chat/collections" -d "name=feedback"
curl -sf -u "${CB_USER}:${CB_PASS}" -X POST "${SCOPES}/chat/collections" -d "name=users"

log "Collections created. Waiting for indexer..."
sleep 5

# ── 10. GSI Indexes ───────────────────────────────────────────────────────────
log "Creating GSI indexes..."

# Backtick character — avoids shell command-substitution when used in double-quoted strings
BT='`'
KS_T="${BT}${CB_BUCKET}${BT}.${BT}transcripts${BT}.${BT}tickets${BT}"
KS_SN="${BT}${CB_BUCKET}${BT}.${BT}transcripts${BT}.${BT}snapshots${BT}"

n1ql() {
    curl -sf -u "${CB_USER}:${CB_PASS}" -X POST "${N1QL}" --data-urlencode "statement=$1"
}

# Tickets indexes (match ensure_cb_indexes() in apps/strabo/app.py)
n1ql "CREATE INDEX IF NOT EXISTS ${BT}idx_tickets_org_customer${BT} \
ON ${KS_T}(organization, customer_url, ticket_id) \
WHERE organization IS NOT MISSING"

n1ql "CREATE INDEX IF NOT EXISTS ${BT}idx_tickets_snap_ids${BT} \
ON ${KS_T}(DISTINCT ARRAY sid FOR sid IN snap_ids END) \
WHERE snap_ids IS NOT MISSING"

n1ql "CREATE INDEX IF NOT EXISTS ${BT}idx_tickets_org_date${BT} \
ON ${KS_T}(organization, created DESC) \
WHERE organization IS NOT MISSING AND ticket_id IS NOT MISSING"

n1ql "CREATE INDEX IF NOT EXISTS ${BT}idx_active_tickets_lower_org_status${BT} \
ON ${KS_T}(LOWER(organization), LOWER(status)) \
WHERE type = 'ticket'"

n1ql "CREATE INDEX IF NOT EXISTS ${BT}idx_scrape_jobs_type_started${BT} \
ON ${KS_T}(type, started_at DESC, status)"

n1ql "CREATE INDEX IF NOT EXISTS ${BT}idx_tickets_deleted_ticket_id${BT} \
ON ${KS_T}(_deleted INCLUDE MISSING, ticket_id)"

# Snapshots indexes
n1ql "CREATE INDEX IF NOT EXISTS ${BT}idx_snaps_org_covering${BT} \
ON ${KS_SN}(organization, customer_url, cluster_id, date, scraped_at, bad_count, warn_count) \
WHERE organization IS NOT MISSING"

n1ql "CREATE INDEX IF NOT EXISTS ${BT}idx_snaps_cluster_date${BT} \
ON ${KS_SN}(cluster_id, date, bad_count, warn_count, node_count)"

n1ql "CREATE INDEX IF NOT EXISTS ${BT}idx_snaps_ticket_ids${BT} \
ON ${KS_SN}(DISTINCT ARRAY tid FOR tid IN ticket_ids END) \
WHERE ticket_ids IS NOT MISSING"

n1ql "CREATE INDEX IF NOT EXISTS ${BT}idx_snaps_type_bad_warn${BT} \
ON ${KS_SN}(type, bad_items, warn_items, cb_version, cluster_name, organization, last_scraped_at)"

n1ql "CREATE INDEX IF NOT EXISTS ${BT}idx_snaps_lower_org_date${BT} \
ON ${KS_SN}(LOWER(organization), date DESC)"

# Primary indexes — assets, brands, chat.profiles (created lazily by the app, pre-created here)
n1ql "CREATE PRIMARY INDEX IF NOT EXISTS \
ON ${BT}${CB_BUCKET}${BT}.${BT}transcripts${BT}.${BT}assets${BT}"

n1ql "CREATE PRIMARY INDEX IF NOT EXISTS \
ON ${BT}${CB_BUCKET}${BT}.${BT}transcripts${BT}.${BT}brands${BT}"

n1ql "CREATE PRIMARY INDEX IF NOT EXISTS \
ON ${BT}${CB_BUCKET}${BT}.${BT}chat${BT}.${BT}profiles${BT}"

log "All indexes created."
log "Couchbase initialisation complete."
