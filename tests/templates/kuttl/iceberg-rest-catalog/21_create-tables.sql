-- Create what existing users have: Iceberg tables, views and plain Hive tables, all created via the
-- Thrift API of the Hive metastore.

CREATE SCHEMA IF NOT EXISTS iceberg_thrift.sales WITH (location = 's3a://lakehouse/sales/');

CREATE TABLE IF NOT EXISTS iceberg_thrift.sales.orders (
  id bigint,
  order_uuid uuid,
  customer ROW(name varchar, email varchar, address ROW(city varchar, zip varchar)),
  items ARRAY(ROW(sku varchar, quantity integer, price decimal(10, 2))),
  tags MAP(varchar, varchar),
  total decimal(12, 2),
  express boolean,
  weight_kg double,
  ordered_at timestamp(6) with time zone,
  updated_at timestamp(6),
  delivery_date date
)
WITH (partitioning = ARRAY['day(ordered_at)']);

-- Two separate inserts, so that the table has multiple snapshots (for time travel). Note that the
-- CREATE TABLE already created an (empty) snapshot.
INSERT INTO iceberg_thrift.sales.orders
SELECT * FROM (
  VALUES
    (
      BIGINT '1',
      UUID '7e1f1c3a-1d9b-4c55-9c39-2a8f3b6e0a01',
      ROW('Alice', 'alice@example.com', ROW('Berlin', '10115')),
      ARRAY[ROW('apple', 3, DECIMAL '0.50'), ROW('banana', 6, DECIMAL '0.25')],
      MAP(ARRAY['channel', 'coupon'], ARRAY['web', 'WELCOME10']),
      DECIMAL '3.00',
      false,
      1.2E0,
      TIMESTAMP '2026-11-01 09:15:00.123456 UTC',
      TIMESTAMP '2026-11-01 09:15:00.123456',
      DATE '2026-11-03'
    ),
    (
      BIGINT '2',
      UUID '7e1f1c3a-1d9b-4c55-9c39-2a8f3b6e0a02',
      ROW('Bob', 'bob@example.com', ROW('Hamburg', '20095')),
      ARRAY[ROW('cherry', 1, DECIMAL '4.99')],
      MAP(ARRAY['channel'], ARRAY['app']),
      DECIMAL '4.99',
      true,
      0.3E0,
      TIMESTAMP '2026-11-01 17:42:13.000001 Europe/Berlin',
      TIMESTAMP '2026-11-01 16:42:13.000001',
      DATE '2026-11-02'
    )
) AS new_orders (id, order_uuid, customer, items, tags, total, express, weight_kg, ordered_at, updated_at, delivery_date)
WHERE id NOT IN (SELECT id FROM iceberg_thrift.sales.orders);

INSERT INTO iceberg_thrift.sales.orders
SELECT * FROM (
  VALUES
    (
      BIGINT '3',
      UUID '7e1f1c3a-1d9b-4c55-9c39-2a8f3b6e0a03',
      ROW('Carla', 'carla@example.com', ROW('Munich', '80331')),
      ARRAY[ROW('date', 2, DECIMAL '2.50'), ROW('elderberry', 1, DECIMAL '3.75')],
      MAP(ARRAY['channel'], ARRAY['store']),
      DECIMAL '8.75',
      false,
      CAST(NULL AS double),
      TIMESTAMP '2026-11-02 08:00:00.000000 UTC',
      TIMESTAMP '2026-11-02 08:00:00.000000',
      CAST(NULL AS date)
    )
) AS new_orders (id, order_uuid, customer, items, tags, total, express, weight_kg, ordered_at, updated_at, delivery_date)
WHERE id NOT IN (SELECT id FROM iceberg_thrift.sales.orders);

-- A Trino view, which Trino stores as Hive view in the Hive metastore
CREATE OR REPLACE VIEW iceberg_thrift.sales.daily_revenue AS
SELECT date(ordered_at AT TIME ZONE 'UTC') AS day, sum(total) AS revenue
FROM iceberg_thrift.sales.orders
GROUP BY 1;

CREATE TABLE IF NOT EXISTS hive.sales.legacy_orders AS
SELECT * FROM (VALUES (1, 'apple')) AS t (id, item);
