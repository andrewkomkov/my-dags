CREATE OR REPLACE FUNCTION dds.dm_restaurants__trigger()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
    BEGIN
        IF (TG_OP = 'INSERT') THEN
	           WITH cte as(
					SELECT id,
					restaurant_id,
					startdate,
					LEAD (startdate) OVER (PARTITION BY restaurant_id ORDER BY startdate ASC) AS new_end_date
					FROM dds.dm_restaurants_
					WHERE enddate IS NULL
					ORDER BY 2 DESC,4 ASC)
				UPDATE dds.dm_restaurants_ c
					SET enddate = cte.new_end_date
					FROM cte
					WHERE c.id = cte.id AND cte.new_end_date IS NOT NULL;
            RETURN NEW;
        END IF;
    END;
$function$
;


CREATE OR REPLACE FUNCTION dds.dm_couriers_trigger()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
    BEGIN
        IF (TG_OP = 'INSERT') THEN
	            WITH cte as(
				SELECT id,
				courier_id,
				startdate,
				LEAD (startdate) OVER (PARTITION BY courier_id ORDER BY startdate ASC) AS new_end_date
				FROM dds.dm_couriers
				WHERE enddate  = '2999-01-01'::timestamp
				ORDER BY 2 DESC,4 ASC)
				UPDATE dds.dm_couriers c
				SET enddate = cte.new_end_date
				FROM cte
				WHERE c.id = cte.id AND cte.new_end_date IS NOT NULL;
            RETURN NEW;
        END IF;
    END;
$function$
;

CREATE OR REPLACE FUNCTION dds.dm_users__trigger()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
    BEGIN
        IF (TG_OP = 'INSERT') THEN
	           WITH cte as(
					SELECT id,
					user_id,
					startdate,
					LEAD (startdate) OVER (PARTITION BY user_id ORDER BY startdate ASC) AS new_end_date
					FROM dds.dm_users_
					WHERE enddate  = '2999-01-01'::timestamp
					ORDER BY 2 DESC,4 ASC)
				UPDATE dds.dm_users_ c
					SET enddate = cte.new_end_date
					FROM cte
					WHERE c.id = cte.id AND cte.new_end_date IS NOT NULL;
            RETURN NEW;
        END IF;
    END;
$function$
;