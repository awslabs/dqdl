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
