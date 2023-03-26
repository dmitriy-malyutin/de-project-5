WITH rates AS (
	SELECT
		d.courier_id,
		avg(d.rate) AS avg_rate
	FROM dds.deliveries d
	GROUP BY courier_id
), co_sum AS (
	SELECT
		r.courier_id,
		d."sum",
		d.delivery_ts,
		EXTRACT(YEAR FROM d.delivery_ts) AS settlement_year,
		EXTRACT(MONTH FROM d.delivery_ts) AS settlement_month,
		CASE
			WHEN r.avg_rate < 4 THEN
				CASE
					WHEN d."sum"*0.05 < 100 THEN 100
					ELSE d."sum"*0.05
				END
			WHEN 4 <= r.avg_rate AND r.avg_rate < 4.5 THEN
				CASE
					WHEN d."sum"*0.07 < 150 THEN 150
					ELSE d."sum"*0.07
				END
			WHEN 4.5 <= r.avg_rate AND r.avg_rate < 4.9 THEN
				CASE
					WHEN d."sum"*0.08 < 175 THEN 175
					ELSE d."sum"*0.08
				END
			WHEN 4.9 <= r.avg_rate THEN
				CASE
					WHEN d."sum"*0.1 < 200 THEN 200
					ELSE d."sum"*0.1
				END
		END AS courier_order_sum
	FROM rates r
		JOIN dds.deliveries d ON r.courier_id = d.courier_id
)
INSERT INTO cdm.dm_courier_ledger(
	courier_id,
	courier_name,
	settlement_year,
	settlement_month,
	orders_count,
	orders_total_sum,
	rate_avg,
	order_processing_fee,
	courier_order_sum,
	courier_tips_sum,
	courier_reward_sum
	)
SELECT
	d.courier_id,
	c."name",
	cs.settlement_year,
	cs.settlement_month,
	count(order_id) AS orders_count,
	sum(d."sum") AS orders_total_sum,
	avg(rate) AS rate_avg,
	sum(d.sum)*0.25 AS order_processing_fee,
	sum(cs.courier_order_sum) AS courier_order_sum,
	sum(d.tip_sum) AS courier_tips_sum,
	sum(cs.courier_order_sum)+sum(d.tip_sum) AS courier_reward_sum
FROM dds.deliveries d
	JOIN dds.couriers c ON c.id = d.courier_id
	JOIN co_sum cs ON cs.courier_id = c.id
		AND d."sum" = cs."sum"
		AND d.delivery_ts = cs.delivery_ts
GROUP BY
	d.courier_id,
	c."name",
	cs.settlement_year,
	cs.settlement_month
ON CONFLICT ON CONSTRAINT dm_courier_ledger_unique DO UPDATE
SET
	courier_name = EXCLUDED.courier_name,
	orders_count = EXCLUDED.orders_count,
	orders_total_sum = EXCLUDED.orders_total_sum,
	rate_avg = EXCLUDED.rate_avg,
	order_processing_fee = EXCLUDED.order_processing_fee,
	courier_order_sum = EXCLUDED.courier_order_sum,
	courier_tips_sum = EXCLUDED.courier_tips_sum,
	courier_reward_sum = EXCLUDED.courier_reward_sum;