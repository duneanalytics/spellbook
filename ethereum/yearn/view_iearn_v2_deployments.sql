BEGIN;
DROP VIEW IF EXISTS iearn_v2."view_iearn_v2_deployments" cascade;

CREATE VIEW iearn_v2."view_iearn_v2_deployments" AS(
    (SELECT
    (wtd."amount"/10^yct."yvault_deposit_token_decimals")*-1 as amount,
    yct."yvault_contract",
    yct."yvault_deposit_token_symbol",
    yct."yvault_deposit_token",
    wtd."call_tx_hash",
    wtd."transaction_type"
    FROM
    (
        (SELECT "amount", "call_tx_hash", "contract_address","call_success",'AAVE' AS transaction_type FROM iearn_v2."yToken_call_withdrawAave")
        UNION ALL
        (SELECT "amount", "call_tx_hash", "contract_address","call_success",'DYDX' AS transaction_type FROM iearn_v2."yToken_call_withdrawDydx")
        UNION ALL
        (SELECT "_amount", "call_tx_hash", "contract_address","call_success",'Compound' AS transaction_type FROM iearn_v2."yToken_call_withdrawSomeCompound")
        UNION ALL
        (SELECT "_amount", "call_tx_hash", "contract_address","call_success",'Fulcrum' AS transaction_type FROM iearn_v2."yToken_call_withdrawSomeFulcrum")
    ) wtd 
    LEFT JOIN yearn."view_yearn_contract_tokens" yct on wtd."contract_address" = yct."yvault_contract"
    WHERE
    "call_success" = True)

    UNION ALL

    (SELECT
    (wtd."amount"/10^yct."yvault_deposit_token_decimals") as amount,
    yct."yvault_contract",
    yct."yvault_deposit_token_symbol",
    yct."yvault_deposit_token",
    wtd."call_tx_hash",
    wtd."transaction_type"
    FROM
    (
        (SELECT "amount", "call_tx_hash", "contract_address","call_success",'AAVE' AS transaction_type FROM iearn_v2."yToken_call_supplyAave")
        UNION ALL
        (SELECT "amount", "call_tx_hash", "contract_address","call_success",'DYDX' AS transaction_type FROM iearn_v2."yToken_call_supplyDydx")
        UNION ALL
        (SELECT "amount", "call_tx_hash", "contract_address","call_success",'Compound' AS transaction_type FROM iearn_v2."yToken_call_supplyCompound")
        UNION ALL
        (SELECT "amount", "call_tx_hash", "contract_address","call_success",'Fulcrum' AS transaction_type FROM iearn_v2."yToken_call_supplyFulcrum")
    ) wtd 
    LEFT JOIN yearn."view_yearn_contract_tokens" yct ON wtd."contract_address" = yct."yvault_contract"
    WHERE
    "call_success" = True)
);
COMMIT;