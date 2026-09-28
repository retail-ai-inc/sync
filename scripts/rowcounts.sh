#!/usr/bin/env bash
#
# Row counts for one sync task: every replicated table or collection with the
# number of rows on each side.
#
#   scripts/rowcounts.sh 41          # MySQL, seconds
#   scripts/rowcounts.sh 39          # MongoDB, minutes -- an exact count on a
#                                    # sharded source asks every shard
#
# The counts are taken when this runs, not collected on a schedule: an exact
# count of both sides of the sharded MongoDB source takes about five minutes,
# which cannot sit on a monitoring interval.
#
# It port-forwards to the deployment because the service is a ClusterIP with no
# ingress, and mints a token because the API takes a Bearer header and has no
# cookie session.
set -euo pipefail

TASK="${1:?usage: rowcounts.sh <task-id> [context] [namespace]}"
CONTEXT="${2:-stg-mongo}"
NAMESPACE="${3:-default}"
PORT="${SYNC_PORT:-18410}"

cleanup() { [[ -n "${PF_PID:-}" ]] && kill "$PF_PID" 2>/dev/null || true; }
trap cleanup EXIT

kubectl --context "$CONTEXT" -n "$NAMESPACE" port-forward deploy/sync "$PORT:8080" >/dev/null 2>&1 &
PF_PID=$!
sleep 4

POD=$(kubectl --context "$CONTEXT" -n "$NAMESPACE" get pods -l app=sync \
        -o jsonpath='{.items[0].metadata.name}')
SECRET=$(kubectl --context "$CONTEXT" -n "$NAMESPACE" exec "$POD" -- \
        sh -c 'printf "%s" "$SYNC_TOKEN_SECRET"')

TOKEN=$(cd "$(dirname "$0")/.." && SYNC_TOKEN_SECRET="$SECRET" go run ./scripts/mint admin admin)

curl -s --max-time 900 -H "Authorization: Bearer $TOKEN" \
     "http://localhost:$PORT/api/sync/$TASK/rowcounts" \
| python3 "$(dirname "$0")/rowcounts_render.py"
