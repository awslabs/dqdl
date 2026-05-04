#!/usr/bin/env bash
set -uo pipefail

usage() {
  cat <<EOF
Usage: backfill_issues.sh <owner/repo> [options]

Trigger the bot's workflow_dispatch on open issues/PRs that the bot
has not yet commented on.

Options:
  --limit N    Max issues to successfully trigger (default: no limit)
  --dry-run    Preview only — list issues, no workflow dispatches
  --delay N    Seconds between triggers (default: 5)
  --help       Show this help
EOF
}

REPO=""
LIMIT=0
DRY_RUN=false
DELAY=5

while [ $# -gt 0 ]; do
  case "$1" in
    --help)    usage; exit 0 ;;
    --dry-run) DRY_RUN=true; shift ;;
    --limit)   [ $# -ge 2 ] || { echo "Error: --limit requires a value" >&2; exit 1; }; LIMIT="$2"; shift 2 ;;
    --delay)   [ $# -ge 2 ] || { echo "Error: --delay requires a value" >&2; exit 1; }; DELAY="$2"; shift 2 ;;
    --*)       echo "Unknown option: $1" >&2; usage >&2; exit 1 ;;
    *)
      if [ -z "$REPO" ]; then
        REPO="$1"; shift
      else
        echo "Unexpected argument: $1" >&2; usage >&2; exit 1
      fi
      ;;
  esac
done

if [ -z "$REPO" ]; then
  usage >&2
  exit 1
fi

# --- Prerequisites ---
if ! command -v gh &>/dev/null; then
  echo "Error: gh CLI not found. Install from https://cli.github.com/" >&2
  exit 2
fi

if ! gh auth status &>/dev/null; then
  echo "Error: gh CLI not authenticated. Run: gh auth login" >&2
  exit 2
fi
