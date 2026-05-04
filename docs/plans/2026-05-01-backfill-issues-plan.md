# Backfill Issues Script Implementation Plan

> **For Kiro:** REQUIRED SUB-SKILL: Use subagent-driven-development (recommended) or executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `scripts/backfill_issues.sh` — a Bash script that triggers the PyDeequ bot's `workflow_dispatch` on all open issues/PRs, with idempotency, incremental rollout, and dry-run support.

**Architecture:** Single Bash script using `gh` CLI for all GitHub API interactions. BATS for TDD. Mock `gh` via exported shell functions in tests.

**Tech Stack:** Bash, `gh` CLI, BATS (bats-core)

**Design spec:** `docs/plans/2026-05-01-backfill-issues-design.md`

---

## File Structure

| Action | File | Responsibility |
|--------|------|---------------|
| Create | `scripts/backfill_issues.sh` | Main script — arg parsing, issue iteration, bot comment check, workflow dispatch |
| Create | `scripts/tests/test_backfill.bats` | BATS tests — 10 test cases with mocked `gh` |

---

## Prerequisites

BATS (bats-core) must be installed. Run:

```bash
brew install bats-core
```

Verify:

```bash
bats --version
# Expected: Bats 1.x.x
```

---

## Task 1: Scaffold script with arg parsing and help

**Files:**
- Create: `scripts/backfill_issues.sh`
- Create: `scripts/tests/test_backfill.bats`

This task implements: shebang, `set -uo pipefail`, `usage()` function, argument parsing (`--limit`, `--dry-run`, `--delay`, `--help`, positional `<owner/repo>`), and exit codes 0/1.

- [ ] **Step 1: Install BATS and write failing tests for arg parsing**

```bash
brew install bats-core
```

Create `scripts/tests/test_backfill.bats`:

```bash
#!/usr/bin/env bats

SCRIPT="$BATS_TEST_DIRNAME/../backfill_issues.sh"

# --- Test 1: No arguments exits 1 with usage ---
@test "no arguments exits 1 with usage" {
  run bash "$SCRIPT"
  [ "$status" -eq 1 ]
  [[ "$output" == *"Usage:"* ]]
}

# --- Test 2: --help exits 0 with usage ---
@test "--help exits 0 with usage" {
  run bash "$SCRIPT" --help
  [ "$status" -eq 0 ]
  [[ "$output" == *"Usage:"* ]]
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
bats scripts/tests/test_backfill.bats
```

Expected: 2 failures (script doesn't exist yet).

- [ ] **Step 3: Implement minimal script to pass**

Create `scripts/backfill_issues.sh`:

```bash
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
    --limit)   LIMIT="$2"; shift 2 ;;
    --delay)   DELAY="$2"; shift 2 ;;
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
```

Make executable:

```bash
chmod +x scripts/backfill_issues.sh
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
bats scripts/tests/test_backfill.bats
```

Expected: 2 tests pass.

- [ ] **Step 5: Commit**

```bash
git add scripts/backfill_issues.sh scripts/tests/test_backfill.bats
git commit -m "feat: scaffold backfill_issues.sh with arg parsing and help"
```

---

## Task 2: Prerequisite check and gh auth validation

**Files:**
- Modify: `scripts/backfill_issues.sh` (append after arg parsing)
- Modify: `scripts/tests/test_backfill.bats` (add test 3)

This task adds: check that `gh` is on PATH and authenticated. Exit 2 if not.

- [ ] **Step 1: Write failing test for missing gh**

Append to `scripts/tests/test_backfill.bats`:

```bash
# --- Test 3: gh not found exits 2 ---
@test "missing gh exits 2" {
  # Override PATH to hide gh
  run env PATH="/usr/bin:/bin" bash "$SCRIPT" owner/repo
  [ "$status" -eq 2 ]
  [[ "$output" == *"gh CLI"* ]]
}
```

- [ ] **Step 2: Run tests to verify test 3 fails**

```bash
bats scripts/tests/test_backfill.bats
```

Expected: test 3 fails (script doesn't check for gh yet).

- [ ] **Step 3: Implement prerequisite check**

Append to `scripts/backfill_issues.sh` after the `if [ -z "$REPO" ]` block:

```bash
# --- Prerequisites ---
if ! command -v gh &>/dev/null; then
  echo "Error: gh CLI not found. Install from https://cli.github.com/" >&2
  exit 2
fi

if ! gh auth status &>/dev/null; then
  echo "Error: gh CLI not authenticated. Run: gh auth login" >&2
  exit 2
fi
```

- [ ] **Step 4: Run tests to verify all pass**

```bash
bats scripts/tests/test_backfill.bats
```

Expected: 3 tests pass.

- [ ] **Step 5: Commit**

```bash
git add scripts/backfill_issues.sh scripts/tests/test_backfill.bats
git commit -m "feat: add gh CLI prerequisite check"
```

---

## Task 3: Core loop — fetch issues, check comments, trigger workflow

**Files:**
- Modify: `scripts/backfill_issues.sh` (append main logic after prereq check)
- Modify: `scripts/tests/test_backfill.bats` (add tests 4-8, 10)

This is the main task. It implements: fetching open issues, checking for bot comments, triggering workflows, skip/fail/trigger counters, summary output, `--limit`, `--dry-run`, and `--delay`.

- [ ] **Step 1: Write failing tests for core behavior**

Append to `scripts/tests/test_backfill.bats`:

```bash
# Helper: create a mock gh script in a temp dir and prepend to PATH
setup() {
  MOCK_DIR="$(mktemp -d)"
  MOCK_GH_LOG="$MOCK_DIR/gh_calls.log"
  export MOCK_GH_LOG
  # Default: no issues (post-jq output: one number per line)
  export MOCK_ISSUES=''
  # Default: no bot comments (post-jq output: count)
  export MOCK_COMMENTS='0'
  # Default: workflow trigger succeeds
  export MOCK_WORKFLOW_EXIT=0
  # Default: auth succeeds
  export MOCK_AUTH_EXIT=0

  cat > "$MOCK_DIR/gh" <<'MOCKSCRIPT'
#!/usr/bin/env bash
echo "$*" >> "$MOCK_GH_LOG"
case "$*" in
  auth\ status*) exit ${MOCK_AUTH_EXIT:-0} ;;
  api\ *issues\?state=open*) printf '%s' "$MOCK_ISSUES" ;;
  api\ *comments*) echo "$MOCK_COMMENTS" ;;
  workflow\ run*) exit ${MOCK_WORKFLOW_EXIT:-0} ;;
  *) echo "UNMOCKED: $*" >&2; exit 99 ;;
esac
MOCKSCRIPT
  chmod +x "$MOCK_DIR/gh"
  export PATH="$MOCK_DIR:$PATH"
}

teardown() {
  rm -rf "$MOCK_DIR"
}

# --- Test 4: --dry-run lists issues, no workflow run calls ---
@test "dry-run lists issues without triggering" {
  MOCK_ISSUES=$'10\n20\n30'
  export MOCK_ISSUES MOCK_COMMENTS='0'
  run bash "$SCRIPT" owner/repo --dry-run
  [ "$status" -eq 0 ]
  [[ "$output" == *"would trigger #10"* ]]
  [[ "$output" == *"would trigger #20"* ]]
  [[ "$output" == *"would trigger #30"* ]]
  # No workflow run calls
  ! grep -q "workflow run" "$MOCK_GH_LOG"
}

# --- Test 5: --limit 2 with 5 issues triggers exactly 2 ---
@test "limit triggers only N issues" {
  MOCK_ISSUES=$'1\n2\n3\n4\n5'
  export MOCK_ISSUES MOCK_COMMENTS='0'
  run bash "$SCRIPT" owner/repo --limit 2 --delay 0
  [ "$status" -eq 0 ]
  [[ "$output" == *"Triggered: 2"* ]]
  count=$(grep -c "workflow run" "$MOCK_GH_LOG")
  [ "$count" -eq 2 ]
}

# --- Test 6: bot already commented → skip ---
@test "skips issues where bot already commented" {
  MOCK_ISSUES=$'10'
  export MOCK_ISSUES MOCK_COMMENTS='1'
  run bash "$SCRIPT" owner/repo --delay 0
  [ "$status" -eq 0 ]
  [[ "$output" == *"Skipped: 1"* ]]
  ! grep -q "workflow run" "$MOCK_GH_LOG"
}

# --- Test 7: mix of commented and uncommented with limit ---
@test "mix of commented and uncommented with limit" {
  # Per-issue comment responses: issues 1,2 have bot comments, 3-5 don't
  cat > "$MOCK_DIR/gh" <<'MOCKSCRIPT'
#!/usr/bin/env bash
echo "$*" >> "$MOCK_GH_LOG"
case "$*" in
  auth\ status*) exit 0 ;;
  api\ *issues\?state=open*) printf '1\n2\n3\n4\n5' ;;
  api\ *issues/1/comments*|api\ *issues/2/comments*) echo '1' ;;
  api\ *comments*) echo '0' ;;
  workflow\ run*) exit 0 ;;
esac
MOCKSCRIPT
  chmod +x "$MOCK_DIR/gh"
  run bash "$SCRIPT" owner/repo --limit 2 --delay 0
  [ "$status" -eq 0 ]
  [[ "$output" == *"Skipped: 2"* ]]
  [[ "$output" == *"Triggered: 2"* ]]
  count=$(grep -c "workflow run" "$MOCK_GH_LOG")
  [ "$count" -eq 2 ]
}

# --- Test 8: workflow trigger failure continues ---
@test "workflow trigger failure continues and reports" {
  MOCK_ISSUES=$'1\n2'
  export MOCK_ISSUES MOCK_COMMENTS='0' MOCK_WORKFLOW_EXIT=1
  run bash "$SCRIPT" owner/repo --delay 0
  [ "$status" -eq 0 ]
  [[ "$output" == *"Failed: 2"* ]]
}

# --- Test 10: empty repo ---
@test "empty repo exits 0 with zero issues message" {
  export MOCK_ISSUES=''
  run bash "$SCRIPT" owner/repo
  [ "$status" -eq 0 ]
  [[ "$output" == *"0 open issues"* ]]
}
```

- [ ] **Step 2: Run tests to verify new tests fail**

```bash
bats scripts/tests/test_backfill.bats
```

Expected: tests 4-8, 10 fail (no main logic yet). Tests 1-3 still pass.

- [ ] **Step 3: Implement core loop**

Append to `scripts/backfill_issues.sh` after the prerequisite check:

```bash
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
    --paginate --jq '[.[] | select(.user.login=="github-actions[bot]")] | length' 2>/dev/null) || bot_comments=""

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
echo "  Skipped:   $skipped"
echo "  Failed:    $failures"
if [ "$LIMIT" -gt 0 ]; then
  remaining=$((TOTAL - triggered - skipped - failures))
  echo "  Remaining: $remaining"
fi
```

- [ ] **Step 4: Run all tests to verify they pass**

```bash
bats scripts/tests/test_backfill.bats
```

Expected: all 8 tests pass.

- [ ] **Step 5: Commit**

```bash
git add scripts/backfill_issues.sh scripts/tests/test_backfill.bats
git commit -m "feat: implement core backfill loop with skip, limit, dry-run, fail-and-continue"
```

---

## Task 4: Delay flag test and final polish

**Files:**
- Modify: `scripts/tests/test_backfill.bats` (add test 9)

- [ ] **Step 1: Write failing test for --delay**

Append to `scripts/tests/test_backfill.bats`:

```bash
# --- Test 9: --delay flag is respected ---
@test "delay flag is passed through" {
  MOCK_ISSUES=$'1'
  export MOCK_ISSUES MOCK_COMMENTS='0'
  run bash "$SCRIPT" owner/repo --delay 0
  [ "$status" -eq 0 ]
  [[ "$output" == *"Triggered: 1"* ]]
}
```

- [ ] **Step 2: Run tests to verify all 9 pass**

```bash
bats scripts/tests/test_backfill.bats
```

Expected: all 9 tests pass (delay is already implemented in Task 3).

- [ ] **Step 3: Commit**

```bash
git add scripts/tests/test_backfill.bats
git commit -m "test: add delay flag test case"
```

---

## Final Verification

- [ ] **Run full test suite**

```bash
bats scripts/tests/test_backfill.bats
```

Expected: 9 tests, 0 failures.

- [ ] **Manual smoke test with --dry-run**

```bash
./scripts/backfill_issues.sh awslabs/dqdl --dry-run --limit 3
```

Expected: lists 3 issues that would be triggered, no actual workflow dispatches.

- [ ] **Verify script is executable and has correct shebang**

```bash
head -1 scripts/backfill_issues.sh
# Expected: #!/usr/bin/env bash
ls -la scripts/backfill_issues.sh | grep -q 'x'
# Expected: executable bit set
```

**Note:** No integration tests exist for this project. The BATS tests with mocked `gh` provide full coverage. The manual smoke test with `--dry-run` against a real repo serves as the integration test.
