#!/usr/bin/env bash
# Absorb one backfill chunk into the main checkout:
#   absorb.sh <tag> <since> <records.json.gz> <d1.sql> <worktree freshness.json> <worktree jev dir>
# - one more geocoding pass over the chunk's restaurants the Census batch
#   could not place (GEOCODE_CAP Nominatim lookups, default 2000, 0 = skip)
# - the pipeline re-reads the records (--input-records): Jev reads their
#   violation lines in decide mode, data.js is re-merged and the D1 SQL is
#   written from the same records, so both outputs carry the same decisions
# - the SQL becomes data/d1_seed_<today>_<tag>_<since>.sql.gz (statement cap
#   checked, sqlite replay); the freshness entries and Jev cache files are
#   copied; only then is data.js moved into place (a rename on the same
#   filesystem) and everything committed in one go (PUSH=1 pushes)
# Every entry point is serialised on one lock, so two absorbs never merge
# from the same data.js.
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
REPO=${DINESCORES_REPO:-$(cd "$HERE/../.." && pwd)}
OUT=${BACKFILL_DIR:-/tmp/dinescores-backfills}; mkdir -p "$OUT"
if [ -z "${ABSORB_LOCKED:-}" ]; then
  exec env ABSORB_LOCKED=1 flock "$OUT/absorb.lock" "$0" "$@"
fi
TAG="$1"; SINCE="$2"; REC="$3"; SQL="$4"; WFRESH="$5"; WJEV="$6"
TODAY=$(date -u +%F)
cd "$REPO"
SEED="data/d1_seed_${TODAY}_${TAG}_${SINCE}.sql.gz"
SQLJ="$OUT/absorb_${TAG}.sql"; rm -f "$SQLJ"
DJS="public/.absorb_${TAG}.data.js"; rm -f "$DJS"          # same filesystem as public/data.js: the mv is a rename
ALOG="$OUT/absorb_${TAG}.log"
trap 'rm -f "$REPO/$DJS"' EXIT

REC_IN="$REC"
if [ "${GEOCODE_CAP:-2000}" -gt 0 ]; then
  GEO="$OUT/absorb_${TAG}.geocoded.records.json.gz"
  if python3 "$HERE/geocode_backlog.py" --patch "$GEO" "${GEOCODE_CAP:-2000}" "$REC" > "$OUT/absorb_${TAG}.geocode.log" 2>&1; then
    grep -E "restaurants without|placed" "$OUT/absorb_${TAG}.geocode.log" | tail -2
    REC_IN="$GEO"
  else
    echo "geocoding pass failed; absorbing the records as fetched"; tail -5 "$OUT/absorb_${TAG}.geocode.log"
  fi
fi

BEFORE=$(python3 -c "import sys;s=open('public/data.js').read();print(s.count('\"i\":'))")
JEV_MODE=${JEV_MODE:-decide} JEV_DECIDE=${JEV_DECIDE:-hazard,summary} \
python3 dinescores_pipeline.py --mode full --input-records "$REC_IN" --output "$OUT/absorb_${TAG}.json" \
  --output-data-js "$DJS" --merge-existing-data-js public/data.js --output-d1-sql "$SQLJ" \
  --jev-deadline-minutes "${JEV_BUDGET_MINUTES:-90}" > "$ALOG" 2>&1 \
  || { echo "absorb merge FAILED"; tail -20 "$ALOG"; exit 1; }
grep -E "Loaded .* records|Jev pass|judged|fell back|Merge:|Embed budget|data.js over|Written|without coordinates" "$ALOG" | cut -c24- | head -16
[ -s "$SQLJ" ] || { echo "the absorb run wrote no SQL; nothing changed"; exit 1; }
AFTER=$(python3 -c "import sys;s=open('$DJS').read();print(s.count('\"i\":'))")
SZ=$(stat -c %s "$DJS")
echo "data.js records: $BEFORE -> $AFTER; bytes $SZ (cap 24,000,000)"
[ "$SZ" -le 24000000 ] || { echo "data.js over cap; nothing changed"; exit 1; }
if [ $((AFTER * 100)) -lt $((BEFORE * 95)) ] && [ -z "${ALLOW_SHRINK:-}" ]; then
  echo "data.js would lose more than 5% of its records ($BEFORE -> $AFTER); nothing changed (ALLOW_SHRINK=1 to accept)"; exit 1
fi

gzip -9 -c "$SQLJ" > "$SEED"
python3 "$HERE/seed_check.py" "$SEED" || { rm -f "$SEED"; exit 1; }
python3 "$HERE/freshness_copy.py" "$WFRESH" "$REC"
if [ -d "$WJEV" ]; then
  for f in "$WJEV"/*.gz; do
    [ -f "$f" ] && [ ! -f "data/jev_judgments/$(basename "$f")" ] && cp "$f" data/jev_judgments/ && echo "  jev cache copied: $(basename "$f")"
  done
fi
mv -f "$DJS" public/data.js
git add "$SEED" public/data.js data/source_freshness.json
git add data/jev_judgments/*.jsonl.gz 2>/dev/null || true
git commit -q -m "Backfill $TAG since $SINCE: D1 seed, data.js merge, freshness"
echo "committed $(git rev-parse --short HEAD)"
if [ -n "${PUSH:-}" ]; then
  BRANCH=$(git rev-parse --abbrev-ref HEAD); ok=
  for i in 1 2 3 4 5; do
    git push -u origin "$BRANCH" >/dev/null 2>&1 && { ok=1; echo pushed; break; }
    sleep $((2**i))
  done
  [ -n "$ok" ] || { echo "push failed after 5 attempts; the commit is local" >&2; exit 1; }
fi
