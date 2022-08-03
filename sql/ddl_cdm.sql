CREATE TABLE IF NOT EXISTS cdm.dm_courier_ledger (
	id serial4 NOT NULL , -- идентификатор записи
	courier_id int4 NOT NULL, --  ID курьера, которому перечисляем.
	courier_name varchar NOT NULL, --  Ф. И. О. курьера.
	settlement_year int4 NOT NULL, -- год отчёта.
	settlement_month int4 NOT NULL, --  месяц отчёта, где 1 — январь и 12 — декабрь.
	orders_count int4 NOT NULL DEFAULT 0, --  количество заказов за период (месяц).
	orders_total_sum numeric(14, 2) NOT NULL DEFAULT 0, -- общая стоимость заказов
	rate_avg numeric(4, 2) NOT NULL DEFAULT 0, -- средний рейтинг курьера по оценкам пользователей.
	order_processing_fee numeric(14, 2) NOT NULL DEFAULT 0, -- сумма, удержанная компанией за обработку заказов, которая высчитывается как orders_total_sum * 0.25
	courier_order_sum numeric(14, 2) NOT NULL DEFAULT 0, -- сумма, которую необходимо перечислить ресторану. Высчитывается как сумма начислений по заказам, доставленным курьером. За каждый доставленный заказ курьер должен получить некоторую сумму в зависимости от рейтинга
	courier_tips_sum numeric(14, 2) NOT NULL DEFAULT 0,
	courier_reward_sum numeric(14, 2) NOT NULL DEFAULT 0
);

-- Column comments

COMMENT ON COLUMN cdm.dm_courier_ledger.id IS 'идентификатор записи';
COMMENT ON COLUMN cdm.dm_courier_ledger.courier_id IS ' ID курьера, которому перечисляем.';
COMMENT ON COLUMN cdm.dm_courier_ledger.courier_name IS ' Ф. И. О. курьера.';
COMMENT ON COLUMN cdm.dm_courier_ledger.settlement_year IS 'год отчёта.';
COMMENT ON COLUMN cdm.dm_courier_ledger.settlement_month IS ' месяц отчёта, где 1 — январь и 12 — декабрь.';
COMMENT ON COLUMN cdm.dm_courier_ledger.orders_count IS ' количество заказов за период (месяц).';
COMMENT ON COLUMN cdm.dm_courier_ledger.orders_total_sum IS 'общая стоимость заказов';
COMMENT ON COLUMN cdm.dm_courier_ledger.rate_avg IS 'средний рейтинг курьера по оценкам пользователей.';
COMMENT ON COLUMN cdm.dm_courier_ledger.order_processing_fee IS 'сумма, удержанная компанией за обработку заказов, которая высчитывается как orders_total_sum * 0.25';
COMMENT ON COLUMN cdm.dm_courier_ledger.courier_order_sum IS 'сумма, которую необходимо перечислить ресторану. Высчитывается как сумма начислений по заказам, доставленным курьером. За каждый доставленный заказ курьер должен получить некоторую сумму в зависимости от рейтинга';