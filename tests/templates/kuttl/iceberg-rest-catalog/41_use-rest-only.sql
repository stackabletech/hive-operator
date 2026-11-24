-- Write and compact via the REST catalog while the Thrift API is disabled. Commits take locks in the
-- Hive metastore, which the embedded REST catalog does in-process, not via the Thrift server.
INSERT INTO iceberg_rest.sales.orders
SELECT * FROM (
  VALUES
    (
      BIGINT '7',
      UUID '7e1f1c3a-1d9b-4c55-9c39-2a8f3b6e0a07',
      ROW('Grace', 'grace@example.com', ROW('Bremen', '28195')),
      ARRAY[ROW('lemon', 2, DECIMAL '0.45')],
      MAP(ARRAY['channel'], ARRAY['store']),
      DECIMAL '0.90',
      true,
      0.2E0,
      TIMESTAMP '2026-11-03 07:00:00.000000 UTC',
      TIMESTAMP '2026-11-03 07:00:00.000000',
      DATE '2026-11-04',
      CAST(NULL AS decimal(5, 2))
    )
) AS new_orders (id, order_uuid, customer, items, tags, total, express, weight_kg, ordered_at, updated_at, delivery_date, discount)
WHERE id NOT IN (SELECT id FROM iceberg_rest.sales.orders);

ALTER TABLE iceberg_rest.sales.orders EXECUTE optimize;
