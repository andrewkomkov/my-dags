CREATE TABLE IF NOT EXISTS dds.couriers_ratings_revenue (
	id serial4 NOT NULL,
	min_revenue numeric(14, 2) NOT NULL,
	min_rating numeric(14, 2) NOT NULL,
	max_rating numeric(14, 2) NOT NULL,
	percent_revenue numeric(14, 2) NOT NULL,
	CONSTRAINT couriers_ratings_revenue_pk PRIMARY KEY (id)
);


CREATE TABLE IF NOT EXISTS dds.dm_addresses (
	id serial4 NOT NULL,
	adress varchar NOT NULL,
	CONSTRAINT dm_adresses_pk PRIMARY KEY (id),
	CONSTRAINT dm_adresses_un UNIQUE (adress)
);


CREATE TABLE IF NOT EXISTS dds.dm_couriers (
	id serial4 NOT NULL,
	courier_id varchar NOT NULL,
	courier_name varchar NOT NULL,
	startdate timestamp NOT NULL DEFAULT now(),
	enddate timestamp NULL DEFAULT '2999-01-01 00:00:00'::timestamp without time zone,
	CONSTRAINT dm_couriers_un UNIQUE (courier_id, courier_name),
	CONSTRAINT dm_dm_couriers PRIMARY KEY (id)
);


CREATE TRIGGER dm_couriers_trigger_ AFTER
INSERT
    ON
    dds.dm_couriers FOR EACH STATEMENT EXECUTE FUNCTION dds.dm_couriers_trigger();


CREATE TABLE IF NOT EXISTS dds.dm_restaurants_ (
	id serial4 NOT NULL,
	restaurant_id varchar NOT NULL,
	restaurant_name varchar NOT NULL,
	startdate timestamp NOT NULL DEFAULT now(),
	enddate timestamp NULL DEFAULT '2999-01-01 00:00:00'::timestamp without time zone,
	CONSTRAINT dm_restaurants__un UNIQUE (restaurant_id, restaurant_name),
	CONSTRAINT pk_dm_restaurants_ PRIMARY KEY (id)
);


CREATE TRIGGER dm_restaurants__trigger_ AFTER
INSERT
    ON
    dds.dm_restaurants_ FOR EACH STATEMENT EXECUTE FUNCTION dds.dm_restaurants__trigger();



CREATE TABLE IF NOT EXISTS dds.dm_users_ (
	id serial4 NOT NULL,
	user_id varchar NOT NULL,
	user_name varchar NOT NULL,
	login varchar NOT NULL,
	startdate timestamp NOT NULL DEFAULT now(),
	enddate timestamp NOT NULL DEFAULT '2999-01-01 00:00:00'::timestamp without time zone,
	CONSTRAINT dm_users__un UNIQUE (user_id, user_name, login),
	CONSTRAINT pk_dm_users_ PRIMARY KEY (id)
);


CREATE TRIGGER dm_users__trigger_ AFTER
INSERT
    ON
    dds.dm_users_ FOR EACH STATEMENT EXECUTE FUNCTION dds.dm_users__trigger();


CREATE TABLE IF NOT EXISTS dds.dm_timestamps (
	id serial4 NOT NULL,
	ts timestamp NOT NULL,
	"year" int2 NOT NULL,
	"month" int2 NOT NULL,
	"day" int2 NOT NULL,
	"time" time NOT NULL,
	"date" date NOT NULL,
	CONSTRAINT dm_timestamps_day_check CHECK (((day >= 1) AND (day <= 31))),
	CONSTRAINT dm_timestamps_month_check CHECK (((month >= 1) AND (month <= 12))),
	CONSTRAINT dm_timestamps_pkey PRIMARY KEY (id),
	CONSTRAINT dm_timestamps_un UNIQUE (ts),
	CONSTRAINT dm_timestamps_year_check CHECK (((year >= 2022) AND (year < 2500)))
);

CREATE TABLE IF NOT EXISTS dds.dm_orders (
	id serial4 NOT NULL,
	user_id int4 NOT NULL,
	restaurant_id int4 NOT NULL,
	timestamp_id int4 NOT NULL,
	order_key varchar NOT NULL,
	order_status varchar NOT NULL,
	CONSTRAINT dm_orders_pkey PRIMARY KEY (id),
	CONSTRAINT dm_orders_un UNIQUE (order_key, restaurant_id, user_id, order_status),
	CONSTRAINT dm_orders_restaraunts_fk FOREIGN KEY (restaurant_id) REFERENCES dds.dm_restaurants_(id),
	CONSTRAINT dm_orders_timestamps_id_fkey FOREIGN KEY (timestamp_id) REFERENCES dds.dm_timestamps(id),
	CONSTRAINT dm_orders_users__fk FOREIGN KEY (user_id) REFERENCES dds.dm_users_(id)
);

CREATE TABLE IF NOT EXISTS dds.dm_deliveries (
	id serial4 NOT NULL,
	delivery_id varchar NOT NULL,
	ts_id int4 NOT NULL,
	order_id int4 NOT NULL,
	courier_id int4 NOT NULL,
	address_id int4 NOT NULL,
	rate int4 NOT NULL,
	sum numeric(14, 2) NOT NULL,
	tip_sum numeric(14, 2) NOT NULL,
	CONSTRAINT dm_deliveries_pk PRIMARY KEY (id),
	CONSTRAINT dm_deliveries_un UNIQUE (delivery_id),
	CONSTRAINT dm_deliveries_addresses_fk FOREIGN KEY (address_id) REFERENCES dds.dm_addresses(id),
	CONSTRAINT dm_deliveries_couriers_fk FOREIGN KEY (courier_id) REFERENCES dds.dm_couriers(id),
	CONSTRAINT dm_deliveries_orders_fk FOREIGN KEY (order_id) REFERENCES dds.dm_orders(id),
	CONSTRAINT dm_deliveries_timestamp_fk FOREIGN KEY (ts_id) REFERENCES dds.dm_timestamps(id)
);