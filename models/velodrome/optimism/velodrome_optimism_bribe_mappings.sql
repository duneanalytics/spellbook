
SELECT
 cg._pool AS pool
, existing_bribe AS incentives_contract
, 'external bribe' as incentives_type
, evt_block_time
, evt_block_number
, contract_address,
, evt_tx_hash
, evt_index

FROM {{ source('velodrome_optimism','WrappedExternalBribeFactory_call_createBribe') }} ccb
    LEFT JOIN {{ source('velodrome_optimism','BribeFactory_call_createExternalBribe') }} ceb
        ON ceb.output_0 = ccb.existing_bribe
    LEFT JOIN {{ source('velodrome_optimism', 'GaugeFactory_call_createGauge') }} cg
        ON cg._external_bribe = ccb.existing_bribe

WHERE ccb.call_success = true