#!/usr/bin/env bash
# usage: wait-for.sh <url> [timeout_seconds]  — waits until the URL answers 2xx
url="$1"; timeout="${2:-60}"; start=$(date +%s)
until curl -sf -o /dev/null "$url"; do
  if (( $(date +%s) - start > timeout )); then echo "timeout waiting for $url" >&2; exit 1; fi
  sleep 1
done
