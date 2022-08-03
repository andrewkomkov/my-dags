INSERT INTO cdm.dm_courier_ledger (courier_id, courier_name, settlement_year, settlement_month, orders_count, orders_total_sum, rate_avg, order_processing_fee, courier_order_sum, courier_tips_sum, courier_reward_sum)
WITH couriers_sum as(
SELECT dd.delivery_id,
CASE WHEN
dd.sum * (crr.percent_revenue/100.00) > crr.min_revenue
THEN dd.sum * (crr.percent_revenue/100.00)
ELSE crr.min_revenue
END AS courier_order_sum
FROM dds.dm_deliveries dd
LEFT JOIN dds.couriers_ratings_revenue crr ON dd.rate >= crr.min_rating AND dd.rate < crr.max_rating)
SELECT DISTINCT
dc.id AS courier_id,
dc.courier_name AS courier_name,
dt."year" AS settlement_year,
dt."month" AS settlement_month,
COUNT(dd.order_id) over(PARTITION BY dc.id) AS orders_count,
sum(dd.sum) OVER (PARTITION BY dc.id) AS orders_total_sum,
avg(dd.rate) OVER (PARTITION BY dc.id) AS rate_avg,
(sum(dd.sum) OVER (PARTITION BY dc.id)) * 0.25 AS order_processing_fee,
sum(cs.courier_order_sum) OVER (PARTITION BY dc.id) :: numeric(14,2) AS courier_order_sum,
sum(dd.tip_sum) OVER (PARTITION BY dc.id) AS courier_tips_sum,
sum(cs.courier_order_sum) OVER (PARTITION BY dc.id) + (sum(dd.tip_sum) OVER (PARTITION BY dc.id) * 0.95) AS courier_reward_sum
FROM dds.dm_couriers dc 
LEFT JOIN dds.dm_deliveries dd ON dc.id = dd.courier_id 
LEFT JOIN dds.dm_orders do2 ON dd.order_id = do2.id
LEFT JOIN dds.dm_timestamps dt ON dd.ts_id = dt.id
LEFT JOIN couriers_sum cs ON cs.delivery_id = dd.delivery_id
WHERE dc.enddate :: date = '2999-01-01'::date AND dt."month" = 7