CREATE OR REPLACE VIEW bancor.view_update_price_data AS
SELECT "_connectorToken" AS connector_token,
        t.symbol AS token_symbol,
        t.decimals,
       "_tokenSupply" AS token_supply,
       "_connectorBalance" AS connector_balance,
       "_connectorWeight" AS connector_weight,
       s.contract_address,
       evt_tx_hash AS tx_hash,
       evt_block_time AS block_time
FROM bancor."BancorConverter_evt_PriceDataUpdate" s
LEFT JOIN erc20.tokens t ON s."_connectorToken" = t.contract_address
;
