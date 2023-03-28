-- dds.couriers

INSERT INTO dds.couriers(id, "name")
SELECT _id, "name"
FROM stg.couriers c
ON CONFLICT (id) DO NOTHING;