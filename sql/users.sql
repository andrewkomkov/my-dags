INSERT INTO dds.dm_users (user_id, user_name, user_login)
SELECT  
object_id AS user_id, 
object_value ::json ->> 'name' AS user_name,
object_value ::json ->> 'login' AS user_login
FROM stg.ordersystem_users