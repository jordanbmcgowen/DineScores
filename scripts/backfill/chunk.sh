#!/usr/bin/env bash
# Run one backfill chunk in a worktree and absorb it into the main checkout.
#
#   chunk.sh <worktree> <tag> <since YYYY-MM-DD> [until YYYY-MM-DD] -- <slug...>
#
# The worktree is a checkout of the pipeline (detached at the commit under
# integration, so several sources can fetch in parallel) with its own copy
# of data/source_freshness.json, seeded from the main checkout on first use
# and carried from chunk to chunk. The chunk writes the full inspection
# records (--output-records, gz), the D1 SQL and the worktree freshness
# file; on success absorb.sh (under a lock) merges the records into the
# main checkout's data.js, writes the seed, copies the freshness entries and
# commits. See README.md in this directory for the environment knobs.
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
REPO=${DINESCORES_REPO:-$(cd "$HERE/../.." && pwd)}
WT="$1"; TAG="$2"; SINCE="$3"; shift 3
UNTIL=""; if [ "${1:-}" != "--" ]; then UNTIL="$1"; shift; fi
[ "${1:-}" = "--" ] && shift
SLUGS=("$@")
[ ${#SLUGS[@]} -gt 0 ] || { echo "usage: chunk.sh <worktree> <tag> <since> [until] -- <slug...>"; exit 2; }
OUT=${BACKFILL_DIR:-/tmp/dinescores-backfills}; mkdir -p "$OUT"
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
BASE="$OUT/${TAG}_${SINCE}_${STAMP}"
LOG="$BASE.log"
cd "$WT"
[ -f data/source_freshness.json ] || cp "$REPO/data/source_freshness.json" data/source_freshness.json
EXTRA=()
[ -n "$UNTIL" ] && EXTRA+=(--until-date "$UNTIL")
echo "== $TAG [${SLUGS[*]}] since $SINCE ${UNTIL:+until $UNTIL} in $WT (log $LOG) start $(date -u +%FT%TZ)"
# shellcheck disable=SC2086
python3 dinescores_pipeline.py --mode full --cities "${SLUGS[@]}" --since-date "$SINCE" "${EXTRA[@]}" \
  --output "$BASE.restaurants.json" --output-records "$BASE.records.json.gz" \
  --output-d1-sql "$BASE.sql" --freshness-file data/source_freshness.json \
  --deadline-minutes "${DEADLINE_MINUTES:-300}" ${EXTRA_ARGS:-} > "$LOG" 2>&1 \
  || { echo "pipeline FAILED ($TAG); tail:"; tail -30 "$LOG"; exit 1; }
grep -E "Fetch outcomes this run" -A 60 "$LOG" | grep -E "(ok|error|empty|skipped|blocked|partial) " | cut -c24- | head -45 || true
echo "== $TAG fetched; absorbing $(date -u +%FT%TZ)"
flock "$OUT/absorb.lock" "$HERE/absorb.sh" "$TAG" "$SINCE" "$BASE.records.json.gz" "$BASE.sql" "$WT/data/source_freshness.json" "$WT/data/jev_judgments"
echo "== $TAG done $(date -u +%FT%TZ)"
