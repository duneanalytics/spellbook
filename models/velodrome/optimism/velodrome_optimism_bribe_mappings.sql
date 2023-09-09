{{
    config(
        schema = 'velodrome_optimism',
        alias=alias('bribe_mappings'),
        tags=['dunesql'],
        materialized = 'table',
        unique_key = ['pool_contract', 'incentives_contract', 'allowed_rewards'],
        post_hook='{{ expose_spells(\'["optimism"]\',
                                    "project",
                                    "velodrome",
                                    \'["msilb7"]\') }}'
    ) 
}}


SELECT
  'optimism' as blockchain
, 'velodrome' AS project
, version
, pool_contract, incentives_contract, incentives_type, allowed_rewards
, evt_block_time, evt_block_number, contract_address, evt_tx_hash, evt_index

FROM (
        SELECT
        '1' as version
        , cg._pool AS pool_contract
        , COALESCE(ccb.output_0, ceb.output_0) AS incentives_contract
        , 'external bribe' as incentives_type
        , ceb.allowedRewards AS allowed_rewards
        ,  COALESCE(ccb.call_block_time,ceb.call_block_time) AS evt_block_time
        ,  COALESCE(ccb.call_block_number,ceb.call_block_number) AS evt_block_number
        ,  COALESCE(ccb.contract_address,ceb.contract_address) AS contract_address
        ,  COALESCE(ccb.call_tx_hash,ceb.call_tx_hash) AS evt_tx_hash
        , 1 AS evt_index

        FROM {{ source('velodrome_optimism','WrappedExternalBribeFactory_call_createBribe') }} ccb
        INNER JOIN {{ source('velodrome_optimism','BribeFactory_call_createExternalBribe') }} ceb
                ON ceb.output_0 = ccb.existing_bribe
        INNER JOIN {{ source('velodrome_optimism', 'GaugeFactory_call_createGauge') }} cg
                ON cg._external_bribe = ceb.output_0

        WHERE ceb.call_success = true

        UNION ALL

        SELECT
        '1' as version
        , cg._pool AS pool_contract
        , cib.output_0 AS incentives_contract
        , 'internal bribe' as incentives_type
        , cib.allowedRewards AS allowed_rewards
        , cib.call_block_time AS evt_block_time
        , cib.call_block_number AS evt_block_number
        , cib.contract_address
        , cib.call_tx_hash AS evt_tx_hash
        , 1 AS evt_index

        FROM {{ source('velodrome_optimism','BribeFactory_call_createInternalBribe') }} cib
        INNER JOIN {{ source('velodrome_optimism', 'GaugeFactory_call_createGauge') }} cg
                ON cg._internal_bribe = cib.output_0

        WHERE cib.call_success = true

        UNION ALL

        SELECT
        '2' as version
        , pool    AS pool_contract
        , gauge  AS incentives_contract
        , 'gauge' as incentives_type
        , NULL AS allowed_rewards
        , evt_block_time
        , evt_block_number
        , contract_address
        , evt_tx_hash
        , evt_index

        FROM {{ source('velodrome_v2_optimism','Voter_evt_GaugeCreated') }} gc
) a