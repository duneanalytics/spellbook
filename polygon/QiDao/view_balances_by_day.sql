BEGIN;
DROP MATERIALIZED VIEW IF EXISTS qidao."view_balances_by_day";

CREATE MATERIALIZED VIEW qidao."view_balances_by_day" AS(
with all_values as ((SELECT * FROM
(SELECT
DISTINCT(t1."collateral_token_symbol") as collateral_token_symbol,
t2."collateral_token_contract"
from
qidao."view_contract_token_label" t1 left join qidao."view_contract_token_label" t2 on t1."qidao_contract" = t2."qidao_contract") tokens
CROSS JOIN
(SELECT date_trunc('day', dd):: date as date
FROM generate_series
        ( '2021-05-01'::timestamp
        , CURRENT_DATE::timestamp
        , '1 day'::interval) dd
) tok2)),

deltas as (
SELECT
    cl."collateral_token_symbol",
    cl."collateral_token_contract",
    cl."price_address",
    date_trunc('day',qa."evt_block_time") as date,
    sum("amount_collateral") as change_in_collateral_amount,
    sum("amount_mai") as change_in_mai_amount
from
    qidao."view_evt_aggregate" qa
    left join (SELECT * FROM qidao."view_contract_token_label" LIMIT 5000) cl on qa."contract_address" = cl."qidao_contract"
where
    transaction_type in ('deposit_collateral','withdraw_collateral','payback_mai','borrow_mai','liquidate_vault')
    and collateral_token_symbol is not NULL
Group by 1,2,3,4
order by date),

final_table as (SELECT
av."date",
av."collateral_token_symbol",
av."collateral_token_contract",
d."price_address",
CASE WHEN d."change_in_collateral_amount" IS NULL THEN 0 ELSE d."change_in_collateral_amount" END as change_in_collateral_amount,
CASE WHEN d."change_in_mai_amount" IS NULL THEN 0 ELSE d."change_in_mai_amount" END as change_in_mai_amount
from
all_values av left join deltas d on av."collateral_token_symbol" = d."collateral_token_symbol" and av."date" = d."date"
order by date
)

SELECT
    ft."date",
    ft."collateral_token_symbol",
    ft."collateral_token_contract",
    ft."price_address",
    SUM(ft."change_in_collateral_amount") as change_in_collateral_amount,
    SUM(SUM("change_in_collateral_amount")) OVER (PARTITION BY ft."collateral_token_symbol",ft."collateral_token_contract" ORDER BY ft."date") as cumulative_collateral_amount,
    AVG("price") * SUM(SUM("change_in_collateral_amount")) OVER (PARTITION BY ft."collateral_token_symbol",ft."collateral_token_contract" ORDER BY ft."date") as collateral_amount_usd,
    SUM(ft."change_in_mai_amount") as change_in_mai_amount,
    SUM(SUM("change_in_mai_amount")) OVER (PARTITION BY ft."collateral_token_symbol",ft."collateral_token_contract" ORDER BY ft."date") as cumulative_amount_mai,
    AVG("price") as collateral_price,
    (AVG("price") * SUM(SUM("change_in_collateral_amount")) OVER (PARTITION BY ft."collateral_token_symbol",ft."collateral_token_contract" ORDER BY ft."date")
    / NULLIF(SUM(SUM("change_in_mai_amount")) OVER (PARTITION BY "collateral_token_symbol","collateral_token_contract" ORDER BY ft."date"),0)
    ) * 100 as collateral_ratio
FROM
    final_table ft
    left join
        (SELECT
            "contract_address",
            date_trunc('day',"minute") as date,
            AVG(price) as price
        from
            prices."usd"
        where
            "contract_address" in (select distinct("price_address") from qidao."view_contract_token_label")
        group by 1,2) pr on (ft."price_address" = pr."contract_address" AND ft."date" = pr."date")
Group by 1,2,3,4
order by ft."date",ft."collateral_token_symbol"
);

INSERT INTO cron.job(schedule, command)
VALUES ('3 * * * *', $$REFRESH MATERIALIZED VIEW qidao.view_balances_by_day;$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
COMMIT;
