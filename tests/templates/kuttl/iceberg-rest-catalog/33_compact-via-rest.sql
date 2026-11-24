-- Add another data file to an existing partition via the Thrift API ...
INSERT INTO iceberg_thrift.sales.orders
SELECT * FROM (
  VALUES
    (
      BIGINT '6',
      UUID '7e1f1c3a-1d9b-4c55-9c39-2a8f3b6e0a06',
      ROW('Frank', 'frank@example.com', ROW('Dresden', '01067')),
      ARRAY[ROW('kiwi', 4, DECIMAL '0.60')],
      MAP(ARRAY['channel'], ARRAY['web']),
      DECIMAL '2.40',
      false,
      0.4E0,
      TIMESTAMP '2026-11-01 11:11:11.111111 UTC',
      TIMESTAMP '2026-11-01 11:11:11.111111',
      DATE '2026-11-04',
      CAST(NULL AS decimal(5, 2))
    )
) AS new_orders (id, order_uuid, customer, items, tags, total, express, weight_kg, ordered_at, updated_at, delivery_date, discount)
WHERE id NOT IN (SELECT id FROM iceberg_thrift.sales.orders);

-- ... and compact it via the REST catalog
ALTER TABLE iceberg_rest.sales.orders EXECUTE optimize;
