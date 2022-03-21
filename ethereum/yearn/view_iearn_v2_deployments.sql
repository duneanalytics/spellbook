sBEGIN;
DROP VIEW IF EXISTS iearn_v2."view_iearn_v2_deployments" cascade;

CREATE VIEW iearn_v2."view_iearn_v2_deployments" AS(
    (SELECT
    ("amount"/10^"yvault_deposit_token_decimals")*-1 as amount,
    "yvault_contract",
    "yvault_deposit_token_symbol",
    "yvault_deposit_token",
    "call_tx_hash",
    "transaction_type"
    FROM
    (
        (SELECT "amount", "call_tx_hash", "contract_address","call_success",'AAVE' as transaction_type FROM iearn_v2."yToken_call_withdrawAave")
        UNION ALL
        (SELECT "amount", "call_tx_hash", "contract_address","call_success",'DYDX' as transaction_type FROM iearn_v2."yToken_call_withdrawDydx")
        UNION ALL
        (SELECT "_amount", "call_tx_hash", "contract_address","call_success",'Compound' as transaction_type FROM iearn_v2."yToken_call_withdrawSomeCompound")
        UNION ALL
        (SELECT "_amount", "call_tx_hash", "contract_address","call_success",'Fulcrum' as transaction_type FROM iearn_v2."yToken_call_withdrawSomeFulcrum")
    ) wtd left join yearn."view_yearn_contract_tokens" yct on wtd."contract_address" = yct."yvault_contract"
    WHERE
    "call_success" = True)

    UNION ALL

    (SELECT
    ("amount"/10^"yvault_deposit_token_decimals") as amount,
    "yvault_contract",
    "yvault_deposit_token_symbol",
    "yvault_deposit_token",
    "call_tx_hash",
    "transaction_type"
    FROM
    (
        (SELECT "amount", "call_tx_hash", "contract_address","call_success",'AAVE' as transaction_type FROM iearn_v2."yToken_call_supplyAave")
        UNION ALL
        (SELECT "amount", "call_tx_hash", "contract_address","call_success",'DYDX' as transaction_type FROM iearn_v2."yToken_call_supplyDydx")
        UNION ALL
        (SELECT "amount", "call_tx_hash", "contract_address","call_success",'Compound' as transaction_type FROM iearn_v2."yToken_call_supplyCompound")
        UNION ALL
        (SELECT "amount", "call_tx_hash", "contract_address","call_success",'Fulcrum' as transaction_type FROM iearn_v2."yToken_call_supplyFulcrum")
    ) wtd left join yearn."view_yearn_contract_tokens" yct on wtd."contract_address" = yct."yvault_contract"
    WHERE
    "call_success" = True)
)
