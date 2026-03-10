#!/bin/bash
# =============================================================================
# DineScores Local Runner
# =============================================================================
# Runs the pipeline for WAF-protected cities (Dallas, and future cities that
# block cloud IPs). Pushes data directly to Firestore from your local machine.
#
# Usage:
#   ./run_local_cities.sh                         # weekly refresh, all local cities
#   ./run_local_cities.sh --cities dallas houston  # specific cities
#   ./run_local_cities.sh --mode full              # full historical pull
#   ./run_local_cities.sh --dry-run                # test without writing to Firestore
#
# Setup (first time only):
#   1. pip install firebase-admin requests
#   2. Download Firebase service account key → save as ~/.dinescores/firebase-key.json
#      (Firebase Console → Project Settings → Service Accounts → Generate new key)
#   3. chmod +x run_local_cities.sh
# =============================================================================

set -euo pipefail

# ── Config ───────────────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PIPELINE="$SCRIPT_DIR/dinescores_pipeline.py"
CREDS="${DINESCORES_FIREBASE_CREDS:-$HOME/.dinescores/firebase-key.json}"
LOG_DIR="$SCRIPT_DIR/logs"
LOG_FILE="$LOG_DIR/local_run_$(date +%Y%m%d_%H%M%S).log"

# Default: DFW metro (all cities via MyHealthDepartment portal)
# Uses direct API calls to the MHD portal (no browser required)
LOCAL_CITIES=("dfw")

MODE="weekly"
DRY_RUN=""
EXTRA_ARGS=()

# ── Argument parsing ─────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --cities)
            shift
            LOCAL_CITIES=()
            while [[ $# -gt 0 && ! "$1" =~ ^-- ]]; do
                LOCAL_CITIES+=("$1")
                shift
            done
            ;;
        --mode)
            MODE="$2"; shift 2 ;;
        --dry-run)
            DRY_RUN="--dry-run"; shift ;;
        *)
            EXTRA_ARGS+=("$1"); shift ;;
    esac
done

# ── Preflight checks ─────────────────────────────────────────────────────────
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  DineScores Local Runner"
echo "  $(date '+%Y-%m-%d %H:%M:%S')"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if [[ ! -f "$PIPELINE" ]]; then
    echo "❌ Pipeline not found: $PIPELINE"
    exit 1
fi

if [[ ! -f "$CREDS" ]]; then
    echo "❌ Firebase credentials not found: $CREDS"
    echo ""
    echo "To fix:"
    echo "  1. Go to: https://console.firebase.google.com/project/healthinspections/settings/serviceaccounts/adminsdk"
    echo "  2. Click 'Generate new private key'"
    echo "  3. Save the file as: $CREDS"
    echo "  (or set DINESCORES_FIREBASE_CREDS=/path/to/key.json)"
    exit 1
fi

mkdir -p "$LOG_DIR"

echo "  Mode:    $MODE"
echo "  Cities:  ${LOCAL_CITIES[*]}"
echo "  Creds:   $CREDS"
echo "  Log:     $LOG_FILE"
[[ -n "$DRY_RUN" ]] && echo "  ⚠️  DRY RUN — no Firestore writes"
echo ""

# ── Run pipeline ─────────────────────────────────────────────────────────────
python3 "$PIPELINE" \
    --mode "$MODE" \
    --cities "${LOCAL_CITIES[@]}" \
    --creds "$CREDS" \
    $DRY_RUN \
    "${EXTRA_ARGS[@]}" \
    2>&1 | tee "$LOG_FILE"

EXIT_CODE=${PIPESTATUS[0]}

echo ""
if [[ $EXIT_CODE -eq 0 ]]; then
    echo "✅ Done! Log saved to: $LOG_FILE"
else
    echo "❌ Pipeline exited with code $EXIT_CODE. Check log: $LOG_FILE"
fi

exit $EXIT_CODE
