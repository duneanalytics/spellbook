BEGIN;
DROP MATERIALIZED VIEW IF EXISTS qidao."view_all_owners" CASCADE;

CREATE MATERIALIZED VIEW qidao."view_all_owners" AS(    

with get_all AS(
    SELECT * FROM qidao."view_evt_aggregate"
    WHERE (transaction_type = 'create_vault' OR transaction_type = 'destroy_vault' OR transaction_type = 'transfer_vault' OR transaction_type = 'ownership_transfer')
    ORDER by evt_block_time, evt_index DESC
    ), 
    
sort AS (
SELECT
  *,
  ROW_NUMBER() OVER(PARTITION BY vaultid, contract_address ORDER BY evt_block_time, evt_index DESC) AS row_number
FROM get_all
), 
all_minus_destroy AS (

    SELECT *, (CASE
    WHEN (transaction_type = 'create_vault') THEN address_one
    WHEN (transaction_type = 'transfer_vault') THEN address_one
    WHEN (transaction_type = 'ownership_transfer') THEN address_one
    ELSE NULL
END) as owner from sort WHERE row_number = 1 
), 
minus_destroy AS (
    SELECT amd.vaultid, amd.contract_address, amd.owner FROM all_minus_destroy amd
    LEFT OUTER JOIN (SELECT * FROM qidao."view_evt_aggregate"
    WHERE transaction_type = 'destroy_vault') ad on amd.vaultid = ad.vaultid and amd.contract_address = ad.contract_address
    WHERE ad.vaultid IS NULL
),
all_transactions AS(
    SELECT vaultid as vault_2, contract_address as ca, amount_collateral as collateral, amount_mai FROM qidao."view_evt_aggregate" qa 
    WHERE (transaction_type = 'deposit_collateral' OR transaction_type = 'liquidate_vault' OR transaction_type = 'withdraw_collateral' OR transaction_type = 'payback_mai' OR transaction_type = 'borrow_mai' ) AND 
    contract_address IN (SELECT contract_address FROM minus_destroy)
    ---LIMIT 100
    ),
    
all_amounts AS (
    SELECT * from minus_destroy md
    LEFT JOIN all_transactions at on md.vaultid = at.vault_2 and md.contract_address = at.ca 

    
    ),
    
sum_all AS(
    SELECT vaultid, contract_address, owner, (CASE WHEN sum(collateral) IS NULL THEN 0 ELSE sum(collateral)END) as vault_total, (CASE WHEN sum(amount_mai) IS NULL THEN 0 ELSE sum(amount_mai)END) as total_mai from all_amounts
    GROUP by vaultid, contract_address, owner
    ),
symbol AS (
    SELECT vaultid, contract_address, owner, (price* vault_total) as vault_total_in_usd, vault_total as vault_total_in_collateral, collateral_token_symbol, total_mai FROM sum_all
    ---SELECT vaultid, contract_address, owner, (price * vault_total) as vault_total_in_usd, vault_total as vault_total_in_collateral, collateral_token_symbol FROM get_price 
    LEFT JOIN (SELECT qidao_contract, collateral_token_symbol, price_address from qidao."view_contract_token_label") get_price on sum_all.contract_address = get_price.qidao_contract
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
    on get_price.price_address = prices.ca
    --where collateral_token_symbol is not null 
    
    
   -- SELECT contract_address as ca, price from prices."usd" where minute = (SELECT MAX(minute) from prices."usd") ) prices on get_price.contract_address =  prices.price_address
    --LEFT JOIN (SELECT qidao_contract, collateral_token_symbol, price as collateral_price on get_price.contract_address = price_table.qidao_contract

)

SELECT * from symbol
)


INSERT INTO cron.job(schedule, command)
VALUES ('0 0-12/12 * * *', $$REFRESH MATERIALIZED VIEW CONCURRENTLY qidao.view_all_owners$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
COMMIT;
