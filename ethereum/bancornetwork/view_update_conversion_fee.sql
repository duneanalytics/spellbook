CREATE OR REPLACE VIEW bancornetwork.view_update_conversion_fee AS
SELECT "_prevFee" AS "previous_fee",
       "_newFee" AS "new_fee",
       contract_address,
       evt_tx_hash AS tx_hash,
       evt_block_time AS block_time
FROM
  (SELECT *
   FROM bancornetwork."BancorConverter_v0.10a_evt_ConversionFeeUpdate"
   UNION ALL
   SELECT *
   FROM bancornetwork."BancorConverter_v0.10b_evt_ConversionFeeUpdate"
   UNION ALL
   SELECT *
   FROM bancornetwork."BancorConverter_v0.11_evt_ConversionFeeUpdate"
   UNION ALL
   SELECT *
   FROM bancornetwork."BancorConverter_v0.13_evt_ConversionFeeUpdate"
   UNION ALL
   SELECT *
   FROM bancornetwork."BancorConverter_v0.14a_evt_ConversionFeeUpdate"
   UNION ALL
   SELECT *
   FROM bancornetwork."BancorConverter_v0.14b_evt_ConversionFeeUpdate"
   UNION ALL
   SELECT *
   FROM bancornetwork."BancorConverter_v0.19_evt_ConversionFeeUpdate"
   UNION ALL
   SELECT *
   FROM bancornetwork."BancorConverter_v0.20_evt_ConversionFeeUpdate"
   UNION ALL
   SELECT *
   FROM bancornetwork."BancorConverter_v0.23_evt_ConversionFeeUpdate"
   UNION ALL
   SELECT *
   FROM bancornetwork."BancorConverter_v0.9_evt_ConversionFeeUpdate"
   UNION ALL
   SELECT *
   FROM bancornetwork."LiquidityPoolV1Converter_v0.28_evt_ConversionFeeUpdate"
   UNION ALL
   SELECT *
   FROM bancornetwork."LiquidityPoolV1Converter_v0.29_evt_ConversionFeeUpdate") s
;
