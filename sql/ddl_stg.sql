---  STAGE ---

CREATE TABLE IF NOT EXISTS stg.api_couriers (
	id serial4 NOT NULL,
	object_id varchar NOT NULL,
	object_ts timestamp NOT NULL DEFAULT now(),
	object_value text NOT NULL,
	CONSTRAINT proj_api_couriers_un UNIQUE (object_value, object_id)
);


CREATE TABLE IF NOT EXISTS stg.api_deliveries (
	id serial4 NOT NULL,
	object_id varchar NOT NULL,
	object_ts timestamp NOT NULL DEFAULT now(),
	object_value text NOT NULL,
	CONSTRAINT proj_api_deliveries_un UNIQUE (object_value, object_id)
);


CREATE TABLE IF NOT EXISTS stg.api_restaurants (
	id serial4 NOT NULL,
	object_id varchar NOT NULL,
	object_ts timestamp NOT NULL DEFAULT now(),
	object_value text NOT NULL,
	CONSTRAINT api_restaurants_un UNIQUE (object_value, object_id)
);


CREATE TABLE IF NOT EXISTS stg.mongo_orders (
	id serial4 NOT NULL,
	object_id varchar NOT NULL,
	object_ts timestamp NOT NULL,
	object_value text NOT NULL,
	CONSTRAINT proj_mongo_orders_un UNIQUE (object_value, object_id)
);


CREATE TABLE IF NOT EXISTS stg.mongo_users (
	id serial4 NOT NULL,
	object_id varchar NOT NULL,
	object_ts timestamp NOT NULL,
	object_value text NOT NULL,
	CONSTRAINT proj_mongo_users_un UNIQUE (object_value, object_id)
);