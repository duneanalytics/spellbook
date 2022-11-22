CREATE OR REPLACE VIEW bancornetwork.view_add_convertible_token AS
WITH convertible_tokens AS (
    SELECT *
    FROM bancornetwork."BancorConverterRegistry_v3_evt_ConvertibleTokenAdded"
    UNION ALL
    SELECT *
    FROM bancornetwork."BancorConverterRegistry_v4_evt_ConvertibleTokenAdded"
    UNION ALL
    SELECT *
    FROM bancornetwork."BancorConverterRegistry_v5_evt_ConvertibleTokenAdded"
    UNION ALL
    SELECT *
    FROM bancornetwork."BancorConverterRegistry_v6_evt_ConvertibleTokenAdded"
    UNION ALL
    SELECT *
    FROM bancornetwork."BancorConverterRegistry_v7_evt_ConvertibleTokenAdded"
)
SELECT "_convertibleToken" AS convertible_token,
       symbol,
       decimals,
       "_smartToken" AS smart_token,
       s.contract_address,
       evt_tx_hash AS tx_hash,
       evt_block_time AS block_time
FROM convertible_tokens s
LEFT JOIN erc20.tokens t ON s."_convertibleToken" = t.contract_address
;
