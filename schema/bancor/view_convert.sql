CREATE OR REPLACE VIEW bancor.view_convert AS
WITH tmp AS
  (SELECT "fromToken" AS source_token_address,
          "toToken" AS target_token_address,
          "trader" AS trader,
          "inputAmount" AS source_token_amount,
          "outputAmount" AS target_token_amount,
          "conversionFee" AS conversion_fee,
          contract_address,
          evt_tx_hash AS tx_hash,
          evt_block_time AS block_time
   FROM bancor."BancorConverterWithFee_evt_Conversion"
   UNION ALL
   SELECT "fromToken" AS source_token_address,
          "toToken" AS target_token_address,
          "trader" AS trader,
          "inputAmount" AS source_token_amount,
          "outputAmount" AS target_token_amount,
          NULL AS conversion_fee,
          contract_address,
          evt_tx_hash AS tx_hash,
          evt_block_time AS block_time
   FROM bancor."BancorLegacyConverter_evt_Conversion"
   UNION ALL
   SELECT "fromToken" AS source_token_address,
          "toToken" AS target_token_address,
          "trader" AS trader,
          "inputAmount" AS source_token_amount,
          "outputAmount" AS target_token_amount,
          NULL AS conversion_fee,
          contract_address,
          evt_tx_hash AS tx_hash,
          evt_block_time AS block_time
   FROM bancor."BancorChange_evt_Change"
),
conversions AS
  (SELECT *
   FROM
     (SELECT *,
             row_number() OVER (PARTITION BY (tx_hash,
                                              source_token_address,
                                              target_token_address) ORDER BY conversion_fee) AS rn
      FROM tmp) q
   WHERE rn = 1
)
SELECT source_token_address,
       t1.symbol AS source_token_symbol,
       target_token_address,
       t2.symbol AS target_token_symbol,
       trader,
       source_token_amount / 10^t1.decimals AS source_token_amount,
       source_token_amount / 10^t1.decimals * p1.price AS source_usd_amount,
       target_token_amount / 10^t2.decimals AS target_token_amount,
       target_token_amount / 10^t2.decimals * p2.price AS target_usd_amount,
       conversion_fee / 10^t2.decimals AS conversion_token_fee,
       conversion_fee / 10^t2.decimals * p2.price AS conversion_usd_fee,
       s.contract_address,
       tx_hash,
       block_time
FROM conversions s
LEFT JOIN
  (SELECT *
   FROM erc20.tokens
   UNION
   SELECT *
   FROM bancor.view_smart_tokens) t1 ON s.source_token_address = t1.contract_address
LEFT JOIN prices.usd p1 ON p1.minute = date_trunc('minute', s.block_time)
    AND p1.symbol = t1.symbol
LEFT JOIN
  (SELECT *
   FROM erc20.tokens
   UNION SELECT *
   FROM bancor.view_smart_tokens) t2 ON s.target_token_address = t2.contract_address
LEFT JOIN prices.usd p2 ON p2.minute = date_trunc('minute', s.block_time)
    AND p2.symbol = t2.symbol
;
