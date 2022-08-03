--- timestamps доставок
INSERT INTO dds.dm_timestamps (ts, "year", "month", "day", "time", "date")
SELECT object_ts  AS ts,
EXTRACT(YEAR FROM object_ts) AS year,
EXTRACT(month FROM object_ts) AS month,
EXTRACT(day FROM object_ts) AS DAY,
object_ts :: time AS time,
object_ts :: date AS date
FROM stg.api_deliveries
WHERE object_ts :: date = '{{ ds }}' ::date
ON CONFLICT (ts) DO NOTHING;

--- timestamps заказов
INSERT INTO dds.dm_timestamps (ts, "year", "month", "day", "time", "date")
WITH cte as(
SELECT object_ts
FROM stg.mongo_orders
WHERE object_ts :: date = '{{ ds }}' ::date
)
SELECT object_ts  AS ts,
EXTRACT(YEAR FROM object_ts) AS year,
EXTRACT(month FROM object_ts) AS month,
EXTRACT(day FROM object_ts) AS DAY,
object_ts :: time AS time,
object_ts :: date AS date
FROM cte
ON CONFLICT (ts) DO NOTHING;