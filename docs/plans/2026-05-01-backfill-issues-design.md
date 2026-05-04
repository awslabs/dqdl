# Backfill Issues Script — Design

**Date:** 2026-05-01
**Status:** Approved
**Location:** `scripts/backfill_issues.sh`
**Tests:** `scripts/tests/test_backfill.bats`

## Problem

The PyDeequ bot (`issue-bot.yml`) responds to new issues and PRs via event triggers. Existing open issues/PRs — ~161 on `awslabs/deequ`, ~123 on `awslabs/python-deequ`, and a smaller set on `awslabs/dqdl` — were never processed. We need a script to trigger the bot's `workflow_dispatch` on these backlog items safely and incrementally.

## Approach

A single Bash script that uses `gh` CLI to list open issues, check for prior bot comments, and trigger `workflow_dispatch` for each unprocessed issue. BATS provides TDD-style testing.

## CLI Interface

```
Usage: backfill_issues.sh <owner/repo> [options]

Required:
  <owner/repo>     GitHub repository (e.g., awslabs/deequ)

Options:
  --limit N        Max issues to successfully trigger, excluding skipped and failed (default: no limit)
  --dry-run        Preview only — list issues, no workflow dispatches
  --delay N        Seconds between triggers (default: 5)
  --help           Show usage
```

## Script Flow

1. Parse arguments. Validate `<owner/repo>` is present.
2. Check prerequisites: `gh` installed and authenticated.
3. Fetch all open issue/PR numbers via `gh api repos/{owner/repo}/issues?state=open&sort=created&direction=asc --paginate`, extracting `.number` with `--jq`.
4. For each issue number (until `triggered` reaches `--limit`):
   a. Query comments via `gh api repos/{owner/repo}/issues/{N}/comments --paginate` with `--jq` filtering for `github-actions[bot]`. Pagination ensures the bot comment is found even on heavily-discussed issues.
   b. If bot already commented → skip, increment `skipped` counter.
   c. If `--dry-run` → print "would trigger #N", increment `would_trigger` counter, continue.
   d. Trigger: `gh workflow run issue-bot.yml --repo {owner/repo} -f issue_number={N} -f dry_run=false`.
   e. If trigger fails → log error, increment `failures` counter, continue to next issue.
   f. Increment `triggered` counter. Sleep `$DELAY` seconds.
5. Print summary: triggered, skipped, failures, remaining (if limited).

## Design Decisions

### Idempotency via bot comments

The bot posts as `github-actions[bot]`. The script checks for this commenter before triggering. Re-running the script skips already-processed issues. This makes `--limit N` a natural rollout mechanism:

1. `--limit 5` → triggers 5
2. Review responses on GitHub
3. `--limit 20` → skips the 5 already done, triggers 20 new
4. No limit → processes the rest

No checkpoint file needed.

### `dry_run=false` hardcoded in workflow trigger

The script always passes `dry_run=false` to the workflow. The script's own `--dry-run` flag controls whether workflows are dispatched at all (local preview). These are separate concerns:

- Script `--dry-run`: "show me what you'd trigger" (still checks comments to filter, but dispatches no workflows)
- Workflow `dry_run`: "run the bot but don't post comments" (controlled separately via manual `gh workflow run` if needed)

### Fail-and-continue

If `gh workflow run` fails for one issue (e.g., transient network error), the script logs the failure and continues. The summary reports the failure count. The user can re-run — idempotency ensures only failed issues are retried.

### `--limit` in `--dry-run` mode

In dry-run mode, `--limit` is ignored — the script lists all issues that would be triggered. This gives the operator a full picture of the backlog before committing to a batch size.

### Argument parsing

A `while` loop over `$@` with `shift`. Handles `--flag value` and `--flag=value` forms. Unknown flags cause exit 1 with usage message.

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success (all triggered or dry-run complete) |
| 1 | Usage error (missing repo, bad arguments) |
| 2 | Missing prerequisites (gh not installed or not authenticated) |

## Error Handling

- `set -uo pipefail` at the top (`-e` is omitted — the main loop handles errors explicitly).
- Inside the loop, both the comment check and the workflow trigger are guarded: failures on either increment the `failures` counter and continue to the next issue.
- Outside the loop, errors are fatal. If fetching the issue list fails, the script aborts immediately — nothing to process.

## Testing Strategy

**Framework:** BATS (Bash Automated Testing System)
**Test file:** `scripts/tests/test_backfill.bats`

Tests mock `gh` by exporting a shell function that captures arguments and returns canned JSON responses. The mock distinguishes between API calls (issue list, comments) and workflow triggers by inspecting arguments.

### Test Cases

| # | Test | Verifies |
|---|------|----------|
| 1 | No arguments | Exits 1, prints usage |
| 2 | `--help` flag | Exits 0, prints usage |
| 3 | `gh` not found | Exits 2, prints prerequisite error |
| 4 | `--dry-run` with 3 issues | Lists all 3, no `workflow run` calls |
| 5 | `--limit 2` with 5 issues | Triggers exactly 2, summary shows 3 remaining |
| 6 | Bot already commented on issue | Skips that issue, triggers others |
| 7 | Mix: 2 commented, 3 not, `--limit 2` | Skips 2, triggers 2, summary correct |
| 8 | `gh workflow run` fails for one issue | Continues, reports 1 failure in summary |
| 9 | `--delay` flag | Passed value used (verified via mock timing or arg capture) |
| 10 | Empty repo (zero open issues) | Exits 0, prints "0 open issues found" |

### Mock Design

```bash
# Mock gh script created in setup(), returns post-jq output
# MOCK_ISSUES: newline-separated issue numbers (e.g., "1\n2\n3")
# MOCK_COMMENTS: bot comment count as integer (e.g., "0" or "1")
case "$*" in
  *"issues?state=open"*)  printf '%s' "$MOCK_ISSUES" ;;
  *"/comments"*)          echo "$MOCK_COMMENTS" ;;
  *"workflow run"*)       exit ${MOCK_WORKFLOW_EXIT:-0} ;;
  *"auth status"*)        exit ${MOCK_AUTH_EXIT:-0} ;;
esac
```

## File Structure

```
scripts/
├── backfill_issues.sh          # The script
└── tests/
    ├── test_backfill.bats      # BATS tests
    └── test_bot.py             # Existing bot tests (unchanged)
```

## Out of Scope

- Checkpoint/resume files — idempotency via bot comments is sufficient
- Passing `dry_run=true` to the workflow — use manual `gh workflow run` for that
- Logging to file — stdout suffices; redirect with `| tee` if needed
- GitHub Actions workflow for backfill — loses interactive rollout control
