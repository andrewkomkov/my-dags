---- наполнение адресов
INSERT INTO dds.dm_addresses (adress) 
SELECT DISTINCT 
object_value :: json ->> 'address'
FROM stg.api_deliveries
WHERE object_ts :: date = '{{ ds }}' ::date
ON CONFLICT (adress) DO NOTHING