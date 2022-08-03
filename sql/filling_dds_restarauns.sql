--- наполнение ресторанов
INSERT INTO dds.dm_restaurants_ (restaurant_id, restaurant_name, startdate)
SELECT object_id AS restaurant_id,
object_value :: json ->> 'name' AS restaurant_name,
object_ts AS startdate
FROM stg.api_restaurants
ON CONFLICT (restaurant_id, restaurant_name) DO NOTHING