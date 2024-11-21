{{
    config(
        schema = 'aerodrome_base',
        alias='bribe_mappings',
        
        materialized = 'table',
        unique_key = ['pool_contract', 'incentives_contract', 'allowed_rewards'],
        post_hook='{{ expose_spells(\'["base"]\',
                                    "project",
                                    "aerodrome",
                                    \'["msilb7"]\') }}'
    ) 
}}


SELECT
  'base' as blockchain
, 'aerodrome' AS project
, version
, pool_contract, incentives_contract, incentives_type
, evt_block_time, evt_block_number, contract_address, evt_tx_hash, evt_index

FROM (
        SELECT
        '1' as version
        , pool    AS pool_contract
        , gauge  AS incentives_contract
        , 'gauge' as incentives_type
        , evt_block_time
        , evt_block_number
        , contract_address
        , evt_tx_hash
        , evt_index

        FROM {{ source('aerodrome_base','Voter_evt_GaugeCreated') }} gc
) a