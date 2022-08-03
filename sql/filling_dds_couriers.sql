--- наполнение курьеров
INSERT INTO dds.dm_couriers (courier_id, courier_name)
SELECT object_id AS courier_id,
object_value :: json ->> 'name' AS courier_name
FROM stg.api_couriers
ON CONFLICT (courier_id, courier_name) DO NOTHING