CREATE OR REPLACE VIEW bancornetwork.view_update_price_data AS
SELECT "_connectorToken" AS connector_token,
       t.symbol AS token_symbol,
       "_tokenSupply" / 10^18 AS token_supply,
       "_connectorBalance" / 10^t.decimals AS connector_balance,
       "_connectorWeight" AS reserve_ratio,
       s.contract_address,
       evt_tx_hash AS tx_hash,
       evt_block_time AS block_time
FROM
  (SELECT *
   FROM bancornetwork."BancorConverter_v0.10a_evt_PriceDataUpdate"
   UNION ALL
   SELECT *
   FROM bancornetwork."BancorConverter_v0.10b_evt_PriceDataUpdate"
   UNION ALL
   SELECT *
   FROM bancornetwork."BancorConverter_v0.11_evt_PriceDataUpdate"
   UNION ALL
   SELECT *
   FROM bancornetwork."BancorConverter_v0.13_evt_PriceDataUpdate"
   UNION ALL
   SELECT *
   FROM bancornetwork."BancorConverter_v0.14a_evt_PriceDataUpdate"
   UNION ALL
   SELECT *
   FROM bancornetwork."BancorConverter_v0.14b_evt_PriceDataUpdate"
   UNION ALL
   SELECT *
   FROM bancornetwork."BancorConverter_v0.19_evt_PriceDataUpdate"
   UNION ALL
   SELECT *
   FROM bancornetwork."BancorConverter_v0.20_evt_PriceDataUpdate"
   UNION ALL
   SELECT *
   FROM bancornetwork."BancorConverter_v0.23_evt_PriceDataUpdate"
   UNION ALL
   SELECT *
   FROM bancornetwork."BancorConverter_v0.9_evt_PriceDataUpdate") s
LEFT JOIN erc20.tokens t ON s."_connectorToken" = t.contract_address
;

-- PriceDataUpdate is deprecated since version 0.28+
