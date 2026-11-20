#!/usr/bin/env sh
# Prints selected columns of the orders table as seen via the given Trino catalog as CSV, so that it
# can be compared with the expected content, e.g. `sh orders.sh iceberg_rest | diff 21_orders.csv -`.
# Usage: sh orders.sh iceberg_rest
set -eu

sh trino-query.sh "
  SELECT
    id,
    customer.name,
    customer.address.city,
    cardinality(items),
    element_at(items, 1).sku,
    element_at(tags, 'channel'),
    total,
    express,
    weight_kg,
    ordered_at,
    updated_at,
    delivery_date
  FROM $1.sales.orders
  ORDER BY id
"
