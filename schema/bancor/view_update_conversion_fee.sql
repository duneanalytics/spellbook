CREATE OR REPLACE VIEW bancor.view_update_conversion_fee AS
SELECT "_prevFee" AS "previous_fee",
       "_newFee" AS "new_fee",
       contract_address,
       evt_tx_hash AS tx_hash,
       evt_block_time AS block_time
FROM bancor."BancorConverter_evt_ConversionFeeUpdate"
;
