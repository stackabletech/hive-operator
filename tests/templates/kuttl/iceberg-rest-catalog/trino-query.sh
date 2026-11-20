#!/usr/bin/env sh
# Executes the given SQL statement(s) in Trino and prints the result as unquoted CSV without header.
# Usage: sh trino-query.sh "SELECT 1"
set -eu

kubectl exec -n "$NAMESPACE" trino-cli-0 -- ./trino-cli \
  --server https://trino-coordinator:8443 \
  --insecure \
  --user admin \
  --output-format CSV_UNQUOTED \
  --execute "$1"
