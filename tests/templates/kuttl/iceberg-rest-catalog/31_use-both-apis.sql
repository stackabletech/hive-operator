-- Use the Thrift API and the REST catalog side by side, e.g. while migrating clients one by one.

-- Schema evolution via the REST catalog, the new column is then used via the Thrift API
ALTER TABLE iceberg_rest.sales.orders ADD COLUMN IF NOT EXISTS discount decimal(5, 2);

INSERT INTO iceberg_thrift.sales.orders
SELECT * FROM (
  VALUES
    (
      BIGINT '4',
      UUID '7e1f1c3a-1d9b-4c55-9c39-2a8f3b6e0a04',
      ROW('Dave', 'dave@example.com', ROW('Cologne', '50667')),
      ARRAY[ROW('fig', 10, DECIMAL '0.80')],
      MAP(ARRAY['channel', 'coupon'], ARRAY['web', 'FIGS']),
      DECIMAL '7.00',
      true,
      0.5E0,
      TIMESTAMP '2026-11-02 12:30:00.000000 UTC',
      TIMESTAMP '2026-11-02 12:30:00.000000',
      DATE '2026-11-03',
      DECIMAL '1.00'
    )
) AS new_orders (id, order_uuid, customer, items, tags, total, express, weight_kg, ordered_at, updated_at, delivery_date, discount)
WHERE id NOT IN (SELECT id FROM iceberg_thrift.sales.orders);

-- New partition via the REST catalog
INSERT INTO iceberg_rest.sales.orders
SELECT * FROM (
  VALUES
    (
      BIGINT '5',
      UUID '7e1f1c3a-1d9b-4c55-9c39-2a8f3b6e0a05',
      ROW('Eve', 'eve@example.com', ROW('Leipzig', '04109')),
      ARRAY[ROW('grape', 1, DECIMAL '2.20'), ROW('honeydew', 1, DECIMAL '3.30')],
      MAP(ARRAY['channel'], ARRAY['app']),
      DECIMAL '5.50',
      false,
      2.0E0,
      TIMESTAMP '2026-11-03 20:00:00.000000 UTC',
      TIMESTAMP '2026-11-03 20:00:00.000000',
      DATE '2026-11-05',
      CAST(NULL AS decimal(5, 2))
    )
) AS new_orders (id, order_uuid, customer, items, tags, total, express, weight_kg, ordered_at, updated_at, delivery_date, discount)
WHERE id NOT IN (SELECT id FROM iceberg_rest.sales.orders);

-- Row-level changes via both APIs, which (for format version 2) write delete files
DELETE FROM iceberg_rest.sales.orders WHERE id = 2;
UPDATE iceberg_thrift.sales.orders
SET express = true, updated_at = TIMESTAMP '2026-11-02 10:00:00.000000'
WHERE id = 3;

-- An Iceberg view, created via the REST catalog
CREATE OR REPLACE VIEW iceberg_rest.sales.express_orders AS
SELECT id, customer.name AS customer, total
FROM iceberg_rest.sales.orders
WHERE express;

-- A new namespace via the REST catalog, without an explicit location
CREATE SCHEMA IF NOT EXISTS iceberg_rest.marketing;
CREATE TABLE IF NOT EXISTS iceberg_rest.marketing.campaigns AS
SELECT * FROM (VALUES (1, 'launch'), (2, 'sale')) AS t (id, name);
