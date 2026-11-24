#!/usr/bin/env sh
# Fails unless the orders table has the same content via the Thrift API and via the REST catalog.
# The content is compared using the row count and an order-independent checksum over all columns.
# Usage: sh compare-catalogs.sh
set -eu

checksum() {
  COLUMN_LIST=$(sh trino-query.sh "
    SELECT array_join(array_agg(column_name ORDER BY ordinal_position), ', ')
    FROM $1.information_schema.columns
    WHERE table_schema = 'sales' AND table_name = 'orders'
  ")
  sh trino-query.sh "SELECT count(*), checksum(ROW($COLUMN_LIST)) FROM $1.sales.orders"
}

THRIFT=$(checksum iceberg_thrift)
REST=$(checksum iceberg_rest)
echo "Thrift API: $THRIFT"
echo "REST catalog: $REST"
test "$THRIFT" = "$REST"
