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

# --- Test 3: gh not found exits 2 ---
@test "missing gh exits 2" {
  # Override PATH to hide gh
  run env PATH="/usr/bin:/bin" bash "$SCRIPT" owner/repo
  [ "$status" -eq 2 ]
  [[ "$output" == *"gh CLI"* ]]
}

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

# --- Test 6: bot already commented -> skip ---
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