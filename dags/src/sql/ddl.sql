-- stg.restaurants

CREATE TABLE IF NOT EXISTS stg.restaurants(
	id Serial PRIMARY KEY,
	_id varchar NOT NULL,
	"name" varchar NOT NULL
);


-- stg.couriers

CREATE TABLE IF NOT EXISTS stg.couriers(
	id Serial PRIMARY KEY,
	_id varchar NOT NULL,
	"name" varchar NOT NULL
);

--stg.deliveries

CREATE TABLE IF NOT EXISTS stg.deliveries(
	order_id varchar PRIMARY KEY,
	order_ts varchar NOT NULL,
	delivery_id varchar NOT NULL,
	courier_id varchar NOT NULL,
	address varchar NOT NULL,
	delivery_ts varchar NOT NULL,
	rate varchar NOT NULL,
	"sum" varchar NOT NULL,
	tip_sum varchar NOT NULL
);

--dds.couriers

CREATE TABLE IF NOT EXISTS dds.couriers(
	id varchar PRIMARY KEY,
	"name" varchar NOT NULL
);


--dds.deliveries

CREATE TABLE IF NOT EXISTS dds.deliveries(
	order_id varchar PRIMARY KEY,
	order_ts timestamp NOT NULL,
	delivery_id varchar NOT NULL,
	courier_id varchar NOT NULL,
	address varchar NOT NULL,
	delivery_ts timestamp NOT NULL,
	rate SMALLINT,
	"sum" numeric(14, 2) NOT NULL,
	tip_sum numeric(14, 2)
);

--cdm.dm_courier_ledger

CREATE TABLE IF NOT EXISTS cdm.dm_courier_ledger(
	id Serial PRIMARY KEY,
	courier_id varchar NOT NULL,
	courier_name varchar NOT NULL,
	settlement_year int NOT NULL,
	settlement_month int NOT NULL,
	orders_count int NOT NULL,
	orders_total_sum numeric(14, 2) NOT NULL,
	rate_avg NUMERIC NOT NULL,
	order_processing_fee numeric(14, 2) NOT NULL,
	courier_order_sum numeric(14, 2) NOT NULL,
	courier_tips_sum numeric(14, 2) NOT NULL,
	courier_reward_sum numeric(14, 2) NOT NULL,
	CONSTRAINT dm_courier_ledger_unique UNIQUE (courier_id, settlement_year, settlement_month)
);