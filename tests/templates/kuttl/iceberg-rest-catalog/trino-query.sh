#!/usr/bin/env sh
# Executes SQL in Trino and prints the results as unquoted CSV without header.
# Usage: sh trino-query.sh "SELECT 1"
#        sh trino-query.sh < statements.sql
set -eu

TRINO_CLI="./trino-cli --server https://trino-coordinator:8443 --insecure --user admin --output-format CSV_UNQUOTED"

if [ $# -gt 0 ]; then
  # shellcheck disable=SC2086 # Word splitting of TRINO_CLI is intended
  kubectl exec -n "$NAMESPACE" trino-cli-0 -- $TRINO_CLI --execute "$1"
else
  # shellcheck disable=SC2086 # Word splitting of TRINO_CLI is intended
  kubectl exec -i -n "$NAMESPACE" trino-cli-0 -- $TRINO_CLI
fi
