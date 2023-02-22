{{
    config(
        schema = 'velodrome_optimism',
        alias='bribe_mappings',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['pool', 'gauge'],
        post_hook='{{ expose_spells(\'["optimism"]\',
                                    "project",
                                    "velodrome",
                                    \'["msilb7"]\') }}'
    ) 
}}


SELECT
  'optimism' as blockchain
, 'velodrome' AS project
, '1' as version
, pool_contract, incentives_contract, incentives_type, 
, evt_block_time, evt_block_number, contract_address, evt_tx_hash, evt_index

FROM (
        SELECT
        cg._pool AS pool_contract
        , ccb.output_0 AS incentives_contract
        , 'external bribe' as incentives_type
        , evt_block_time
        , evt_block_number
        , contract_address
        , evt_tx_hash
        , evt_index

        FROM {{ source('velodrome_optimism','WrappedExternalBribeFactory_call_createBribe') }} ccb
        INNER JOIN {{ source('velodrome_optimism', 'GaugeFactory_call_createGauge') }} cg
                ON cg._external_bribe = ccb.existing_bribe

        WHERE ccb.call_success = true

        UNION ALL

        SELECT
        cg._pool AS pool_contract
        , cib.existing_bribe AS incentives_contract
        , 'internal bribe' as incentives_type
        , evt_block_time
        , evt_block_number
        , contract_address
        , evt_tx_hash
        , evt_index

        FROM {{ source('velodrome_optimism','BribeFactory_call_createInternalBribe') }} cib
        LEFT JOIN {{ source('velodrome_optimism', 'GaugeFactory_call_createGauge') }} cg
                ON cg._internal_bribe = cib.existing_bribe

        WHERE ccb.call_success = true
) a