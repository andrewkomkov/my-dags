INSERT INTO dds.dm_users_ (user_id, user_name,login,startdate)
SELECT object_id AS user_id,
object_value :: json ->> 'name' AS user_name,
object_value :: json ->> 'login' AS login,
object_ts AS startdate
FROM stg.mongo_users
WHERE object_ts :: date = '{{ ds }}' ::date
ON CONFLICT (user_id, user_name,login) DO NOTHING