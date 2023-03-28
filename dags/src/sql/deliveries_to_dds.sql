-- dds.deliveries

INSERT INTO dds.deliveries(order_id, order_ts, delivery_id, courier_id, address, delivery_ts, rate, "sum", tip_sum)
SELECT
	order_id,
	CAST(order_ts AS timestamp),
	delivery_id,
	courier_id,
	address,
	CAST(delivery_ts AS timestamp),
	rate,
	CAST("sum" AS NUMERIC(14, 2)),
	CAST(tip_sum AS numeric(14, 2))
FROM stg.deliveries
ON CONFLICT (order_id) DO NOTHING;