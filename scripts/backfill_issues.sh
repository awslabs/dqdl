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

# --- Fetch open issues ---
WORKFLOW="issue-bot.yml"

ISSUES=$(gh api "repos/$REPO/issues?state=open&sort=created&direction=asc" \
  --paginate --jq '.[].number') || {
  echo "Error: failed to fetch issues for $REPO" >&2
  exit 1
}

if [ -z "$ISSUES" ]; then
  echo "0 open issues found for $REPO"
  exit 0
fi

TOTAL=$(echo "$ISSUES" | wc -l | tr -d ' ')
echo "Found $TOTAL open issues/PRs for $REPO"

# --- Process each issue ---
triggered=0
skipped=0
failures=0

for NUM in $ISSUES; do
  # Check limit (counts successful triggers only)
  if [ "$LIMIT" -gt 0 ] && [ "$triggered" -ge "$LIMIT" ]; then
    break
  fi

  # Check for existing bot comment
  bot_comments=$(gh api "repos/$REPO/issues/$NUM/comments" \
    --paginate --jq '[.[] | select(.user.login=="github-actions[bot]")] | length' 2>/dev/null \
    | awk '{s+=$1} END {print s+0}') || bot_comments=""

  if [ -z "$bot_comments" ]; then
    echo "  #$NUM — failed to check comments, skipping"
    failures=$((failures + 1))
    continue
  fi

  if [ "$bot_comments" -gt 0 ]; then
    echo "  #$NUM — bot already commented, skipping"
    skipped=$((skipped + 1))
    continue
  fi

  # Dry run: preview only
  if [ "$DRY_RUN" = true ]; then
    echo "  #$NUM — would trigger"
    continue
  fi

  # Trigger workflow
  if gh workflow run "$WORKFLOW" --repo "$REPO" \
    -f issue_number="$NUM" -f dry_run=false 2>/dev/null; then
    echo "  #$NUM — triggered"
    triggered=$((triggered + 1))
    sleep "$DELAY"
  else
    echo "  #$NUM — trigger FAILED"
    failures=$((failures + 1))
  fi
done

# --- Summary ---
echo ""
echo "Done."
echo "  Triggered: $triggered"
echo "  Skipped: $skipped"
echo "  Failed: $failures"
if [ "$LIMIT" -gt 0 ]; then
  remaining=$((TOTAL - triggered - skipped - failures))
  echo "  Remaining: $remaining"
fi
