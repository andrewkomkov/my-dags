INSERT INTO dds.dm_deliveries (delivery_id, ts_id, order_id, courier_id, address_id, rate, sum, tip_sum)
WITH cte as(
SELECT 
object_id AS delivery_id,
object_ts AS delivery_ts,
object_value :: json ->> 'order_id' AS order_id,
object_value :: json ->> 'courier_id' AS courier_id,
object_value :: json ->> 'address' AS address,
object_value :: json ->> 'rate' AS rate,
object_value :: json ->> 'sum' AS sum,
object_value :: json ->> 'tip_sum' AS tip_sum
FROM stg.api_deliveries
WHERE object_ts :: date = '{{ ds }}' ::date)
SELECT c.delivery_id,
dt.id AS ts_id,
do2.id AS order_id,
dc.id AS courier_id,
da.id AS address_id,
c.rate :: int,
c.sum :: numeric,
c.tip_sum :: numeric
FROM cte c
LEFT JOIN dds.dm_timestamps dt ON c.delivery_ts = dt.ts 
LEFT JOIN dds.dm_orders do2 ON c.order_id = do2.order_key 
LEFT JOIN dds.dm_couriers dc ON c.courier_id = dc.courier_id 
LEFT JOIN dds.dm_addresses da ON c.address = da.adress
ON CONFLICT (delivery_id) DO NOTHING