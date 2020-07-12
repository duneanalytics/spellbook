CREATE OR REPLACE VIEW bancornetwork.view_convert AS
WITH conversions AS (
    SELECT *
    FROM bancornetwork."BancorNetwork_v6_evt_Conversion"
    UNION ALL
    SELECT *
    FROM bancornetwork."BancorNetwork_v7_evt_Conversion"
    UNION ALL
    SELECT *
    FROM bancornetwork."BancorNetwork_v8_evt_Conversion"
    UNION ALL
    SELECT *
    FROM bancornetwork."BancorNetwork_v9_evt_Conversion"
    UNION ALL
    SELECT *
    FROM bancornetwork."BancorNetwork_v10_evt_Conversion"
)
SELECT "_smartToken" AS smart_token_address,
       "_fromToken" AS source_token_address,
       t1.symbol AS source_token_symbol,
       "_toToken" AS target_token_address,
       t2.symbol AS target_token_symbol,
       "_fromAmount" / 10^t1.decimals AS source_token_amount,
       "_toAmount" / 10^t2.decimals AS target_token_amount,
       "_fromAmount" AS source_token_amount_raw,
       "_toAmount" AS target_token_amount_raw,
       "_trader" AS trader,
       s.contract_address,
       evt_tx_hash AS tx_hash,
       evt_index,
       evt_block_time AS block_time
FROM conversions s
LEFT JOIN erc20.tokens t1 ON s."_fromToken" = t1.contract_address
LEFT JOIN erc20.tokens t2 ON s."_toToken" = t2.contract_address
;
