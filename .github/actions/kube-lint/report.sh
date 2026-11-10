#!/usr/bin/env bash
# Append a failed check to the job summary: report.sh <title> <output> [note]
set -euo pipefail
title=$1
output=$2
note=${3:-}
{
  echo "## $title"
  echo ""
  if [ -n "$note" ]; then
    echo "*$note*"
  fi
  echo '```text'
  printf '%s\n' "$output"
  echo '```'
  echo ""
} >> "$GITHUB_STEP_SUMMARY"
