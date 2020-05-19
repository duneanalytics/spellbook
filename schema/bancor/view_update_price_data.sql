CREATE OR REPLACE VIEW bancor.view_update_price_data AS
SELECT "_connectorToken" AS connector_token,
       t.symbol AS token_symbol,
       "_tokenSupply" / 10^18 AS token_supply,
       "_connectorBalance" / 10^t.decimals AS connector_balance,
       "_connectorWeight" AS reserve_ratio,
       s.contract_address,
       evt_tx_hash AS tx_hash,
       evt_block_time AS block_time
FROM bancor."BancorConverter_evt_PriceDataUpdate" s
LEFT JOIN erc20.tokens t ON s."_connectorToken" = t.contract_address
;

-- bancor pool tokens always have 18 decimals
