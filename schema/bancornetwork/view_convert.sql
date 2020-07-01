CREATE OR REPLACE VIEW bancornetwork.view_convert AS
WITH conversions AS
  (SELECT "_fromToken" AS source_token_address,
          "_toToken" AS target_token_address,
          "_trader" AS trader,
          "_amount" AS source_token_amount,
          "_return" AS target_token_amount,
          NULL::numeric AS conversion_fee,
          contract_address,
          evt_tx_hash AS tx_hash,
          evt_block_time AS block_time,
          evt_index
   FROM bancornetwork."BancorChanger_v0.1_evt_Change"
   UNION ALL
   SELECT "_fromToken" AS source_token_address,
          "_toToken" AS target_token_address,
          "_trader" AS trader,
          "_amount" AS source_token_amount,
          "_return" AS target_token_amount,
          NULL::numeric AS conversion_fee,
          contract_address,
          evt_tx_hash AS tx_hash,
          evt_block_time AS block_time,
          evt_index
   FROM bancornetwork."BancorChanger_v0.2_evt_Change"
   UNION ALL
   SELECT "_fromToken" AS source_token_address,
          "_toToken" AS target_token_address,
          "_trader" AS trader,
          "_amount" AS source_token_amount,
          "_return" AS target_token_amount,
          "_conversionFee" AS conversion_fee,
          contract_address,
          evt_tx_hash AS tx_hash,
          evt_block_time AS block_time,
          evt_index
   FROM bancornetwork."BancorConverter_v0.10a_evt_Conversion"
   UNION ALL
   SELECT "_fromToken" AS source_token_address,
          "_toToken" AS target_token_address,
          "_trader" AS trader,
          "_amount" AS source_token_amount,
          "_return" AS target_token_amount,
          "_conversionFee" AS conversion_fee,
          contract_address,
          evt_tx_hash AS tx_hash,
          evt_block_time AS block_time,
          evt_index
   FROM bancornetwork."BancorConverter_v0.10b_evt_Conversion"
   UNION ALL
   SELECT "_fromToken" AS source_token_address,
          "_toToken" AS target_token_address,
          "_trader" AS trader,
          "_amount" AS source_token_amount,
          "_return" AS target_token_amount,
          "_conversionFee" AS conversion_fee,
          contract_address,
          evt_tx_hash AS tx_hash,
          evt_block_time AS block_time,
          evt_index
   FROM bancornetwork."BancorConverter_v0.11_evt_Conversion"
   UNION ALL
   SELECT "_fromToken" AS source_token_address,
          "_toToken" AS target_token_address,
          "_trader" AS trader,
          "_amount" AS source_token_amount,
          "_return" AS target_token_amount,
          "_conversionFee" AS conversion_fee,
          contract_address,
          evt_tx_hash AS tx_hash,
          evt_block_time AS block_time,
          evt_index
   FROM bancornetwork."BancorConverter_v0.13_evt_Conversion"
   UNION ALL
   SELECT "_fromToken" AS source_token_address,
          "_toToken" AS target_token_address,
          "_trader" AS trader,
          "_amount" AS source_token_amount,
          "_return" AS target_token_amount,
          "_conversionFee" AS conversion_fee,
          contract_address,
          evt_tx_hash AS tx_hash,
          evt_block_time AS block_time,
          evt_index
   FROM bancornetwork."BancorConverter_v0.14a_evt_Conversion"
   UNION ALL
   SELECT "_fromToken" AS source_token_address,
          "_toToken" AS target_token_address,
          "_trader" AS trader,
          "_amount" AS source_token_amount,
          "_return" AS target_token_amount,
          "_conversionFee" AS conversion_fee,
          contract_address,
          evt_tx_hash AS tx_hash,
          evt_block_time AS block_time,
          evt_index
   FROM bancornetwork."BancorConverter_v0.14b_evt_Conversion"
   UNION ALL
   SELECT "_fromToken" AS source_token_address,
          "_toToken" AS target_token_address,
          "_trader" AS trader,
          "_amount" AS source_token_amount,
          "_return" AS target_token_amount,
          "_conversionFee" AS conversion_fee,
          contract_address,
          evt_tx_hash AS tx_hash,
          evt_block_time AS block_time,
          evt_index
   FROM bancornetwork."BancorConverter_v0.19_evt_Conversion"
   UNION ALL
   SELECT "_fromToken" AS source_token_address,
          "_toToken" AS target_token_address,
          "_trader" AS trader,
          "_amount" AS source_token_amount,
          "_return" AS target_token_amount,
          "_conversionFee" AS conversion_fee,
          contract_address,
          evt_tx_hash AS tx_hash,
          evt_block_time AS block_time,
          evt_index
   FROM bancornetwork."BancorConverter_v0.20_evt_Conversion"
   UNION ALL
   SELECT "_fromToken" AS source_token_address,
          "_toToken" AS target_token_address,
          "_trader" AS trader,
          "_amount" AS source_token_amount,
          "_return" AS target_token_amount,
          "_conversionFee" AS conversion_fee,
          contract_address,
          evt_tx_hash AS tx_hash,
          evt_block_time AS block_time,
          evt_index
   FROM bancornetwork."BancorConverter_v0.23_evt_Conversion"
   UNION ALL
   SELECT "_fromToken" AS source_token_address,
          "_toToken" AS target_token_address,
          "_trader" AS trader,
          "_amount" AS source_token_amount,
          "_return" AS target_token_amount,
          NULL::numeric AS conversion_fee,
          contract_address,
          evt_tx_hash AS tx_hash,
          evt_block_time AS block_time,
          evt_index
   FROM bancornetwork."BancorConverter_v0.4_evt_Conversion"
   UNION ALL
   SELECT "_fromToken" AS source_token_address,
          "_toToken" AS target_token_address,
          "_trader" AS trader,
          "_amount" AS source_token_amount,
          "_return" AS target_token_amount,
          NULL::numeric AS conversion_fee,
          contract_address,
          evt_tx_hash AS tx_hash,
          evt_block_time AS block_time,
          evt_index
   FROM bancornetwork."BancorConverter_v0.5_evt_Conversion"
   UNION ALL
   SELECT "_fromToken" AS source_token_address,
          "_toToken" AS target_token_address,
          "_trader" AS trader,
          "_amount" AS source_token_amount,
          "_return" AS target_token_amount,
          NULL::numeric AS conversion_fee,
          contract_address,
          evt_tx_hash AS tx_hash,
          evt_block_time AS block_time,
          evt_index
   FROM bancornetwork."BancorConverter_v0.6_evt_Conversion"
   UNION ALL
   SELECT "_fromToken" AS source_token_address,
          "_toToken" AS target_token_address,
          "_trader" AS trader,
          "_amount" AS source_token_amount,
          "_return" AS target_token_amount,
          "_conversionFee" AS conversion_fee,
          contract_address,
          evt_tx_hash AS tx_hash,
          evt_block_time AS block_time,
          evt_index
   FROM bancornetwork."BancorConverter_v0.7_evt_Conversion"
   UNION ALL
   SELECT "_fromToken" AS source_token_address,
          "_toToken" AS target_token_address,
          "_trader" AS trader,
          "_amount" AS source_token_amount,
          "_return" AS target_token_amount,
          "_conversionFee" AS conversion_fee,
          contract_address,
          evt_tx_hash AS tx_hash,
          evt_block_time AS block_time,
          evt_index
   FROM bancornetwork."BancorConverter_v0.8_evt_Conversion"
   UNION ALL
   SELECT "_fromToken" AS source_token_address,
          "_toToken" AS target_token_address,
          "_trader" AS trader,
          "_amount" AS source_token_amount,
          "_return" AS target_token_amount,
          "_conversionFee" AS conversion_fee,
          contract_address,
          evt_tx_hash AS tx_hash,
          evt_block_time AS block_time,
          evt_index
   FROM bancornetwork."BancorConverter_v0.9_evt_Conversion"
   UNION ALL
   SELECT "_fromToken" AS source_token_address,
          "_toToken" AS target_token_address,
          "_trader" AS trader,
          "_amount" AS source_token_amount,
          "_return" AS target_token_amount,
          "_conversionFee" AS conversion_fee,
          contract_address,
          evt_tx_hash AS tx_hash,
          evt_block_time AS block_time,
          evt_index
   FROM bancornetwork."LiquidityPoolV1Converter_v0.28_evt_Conversion"
),
tokens AS
  (SELECT *
   FROM erc20.tokens
   UNION
   SELECT *
   FROM bancornetwork.view_smart_tokens
)
SELECT source_token_address,
       t1.symbol AS source_token_symbol,
       target_token_address,
       t2.symbol AS target_token_symbol,
       trader,
       source_token_amount / 10^t1.decimals AS source_token_amount,
       target_token_amount / 10^t2.decimals AS target_token_amount,
       conversion_fee / 10^t2.decimals AS conversion_token_fee,
       s.contract_address,
       tx_hash,
       block_time,
       evt_index
FROM conversions s
LEFT JOIN tokens t1 ON s.source_token_address = t1.contract_address
LEFT JOIN tokens t2 ON s.target_token_address = t2.contract_address
;
