CREATE OR REPLACE VIEW bancornetwork.view_update_token_rate AS
SELECT "_token1" AS source_token_address,
       "_token2" AS target_token_address,
       "_rateN" AS rate_numerator,
       "_rateD" AS rate_denominator,
       contract_address,
       evt_tx_hash AS tx_hash,
       evt_block_time AS block_time
FROM
  (SELECT *
   FROM bancornetwork."LiquidityPoolV1Converter_v0.28_evt_TokenRateUpdate"
   UNION ALL
   SELECT *
   FROM bancornetwork."LiquidityPoolV1Converter_v0.29_evt_TokenRateUpdate") s
;
