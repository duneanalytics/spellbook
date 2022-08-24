BEGIN;
DROP MATERIALIZED VIEW IF EXISTS qidao."view_all_owners";
CREATE MATERIALIZED VIEW qidao."view_all_owners" AS(

WITH cte AS
(
SELECT
     "vaultid",
     "contract_address",
     "address_one"
FROM(
    SELECT
        DISTINCT ON ("vaultid","contract_address")
         "vaultid",
         "contract_address",
         "address_one",
         "evt_block_time",
         "evt_index"
    FROM
        qidao."view_evt_aggregate"
    WHERE
        transaction_type in ('create_vault','destroy_vault','transfer_vault','transfer')
    ORDER BY "vaultid","contract_address","address_one", "evt_block_time" DESC, "evt_index" ASC) last_tx
WHERE
    vaultid IS NOT null
)
SELECT
    cte."vaultid",
    cte."contract_address",
    cte."address_one" as owner,
    contracts."collateral_token_symbol",
    amounts."amount_mai",
    amounts."amount_collateral",
    prices."price" * "amount_collateral" as collateral_amount_usd,
    ((prices."price" * "amount_collateral")/CASE WHEN "amount_mai" = 0 then NULL ELSE "amount_mai" END)*100 as collateral_ratio
FROM cte left join (
    SELECT
        "vaultid",
        "contract_address",
        sum(amount_mai) as amount_mai,
        sum(amount_collateral) as amount_collateral
    FROM qidao."view_evt_aggregate"
    WHERE
        transaction_type in ('deposit_collateral','withdraw_collateral','payback_mai','borrow_mai','liquidate_vault')
    group by 1,2
    ) amounts on (cte."contract_address" = amounts."contract_address" and cte."vaultid" = amounts."vaultid")
    left join (
    SELECT
        "qidao_contract" as contract_address,
        "collateral_token_symbol",
        "price_address"
    FROM
        qidao."view_contract_token_label"

    ) contracts on (cte."contract_address" = contracts."contract_address")
    LEFT JOIN (
    select
        DISTINCT on ("contract_address") contract_address as ca,
        "minute",
        price
    from
        prices."usd"
    where
        "contract_address" in (select distinct("price_address") from qidao."view_contract_token_label")
    order by contract_address,"minute" desc) prices
    on contracts.price_address = prices.ca
);
COMMIT;

INSERT INTO cron.job(schedule, command)
VALUES ('3 * * * *', $$REFRESH MATERIALIZED VIEW qidao.view_all_owners;$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
COMMIT;
