INSERT INTO dds.dm_orders (user_id, restaurant_id, timestamp_id, order_key, order_status)
WITH cte AS(SELECT
CAST (CAST (object_value :: json ->> 'user' AS json) ->> 'id' AS json) ->> '$oid' AS user_id,
CAST (CAST (object_value :: json ->> 'restaurant' AS json) ->> 'id' AS json) ->> '$oid' AS restaurant_id,
object_ts  AS object_ts,
object_id AS order_key,
CAST(CAST(mo.object_value :: json ->> 'statuses' AS json) ->> 0 AS json) ->> 'status' AS order_status
FROM stg.mongo_orders mo
WHERE object_ts :: date = '{{ ds }}' ::date
)
SELECT 
du.id AS user_id,
dr.id AS restaurant_id,
dt.id AS timestamp_id,
c.order_key,
c.order_status
FROM cte c
LEFT JOIN dds.dm_users_ du ON du.user_id = c.user_id
LEFT JOIN dds.dm_restaurants_ dr ON dr.restaurant_id = c.restaurant_id
LEFT JOIN dds.dm_timestamps dt ON dt.ts = c.object_ts
ON CONFLICT (user_id, restaurant_id, order_key, order_status) DO NOTHING;