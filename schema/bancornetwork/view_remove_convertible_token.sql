CREATE OR REPLACE VIEW bancornetwork.view_remove_convertible_token AS
SELECT "_convertibleToken" AS convertible_token,
       symbol,
       decimals,
       "_smartToken" AS smart_token,
       s.contract_address,
       evt_tx_hash AS tx_hash,
       evt_block_time AS block_time
FROM bancornetwork."ConverterRegistry_evt_ConvertibleTokenRemoved" s
LEFT JOIN erc20.tokens t ON s."_convertibleToken" = t.contract_address
;
