{{ config(
        tags=['dunesql']
        , schema = 'op_governance_optimism'
        , alias = alias('delegates')
        , materialized = 'incremental'
        , file_format = 'delta'
        , incremental_strategy = 'merge'
        , unique_key = ['block_time', 'tx_hash', 'evt_index']
  )
}}


SELECT evt_tx_hash AS tx_hash, 
evt_block_time AS block_time, 
evt_block_number AS block_number,
evt_index,
fromDelegate,
toDelegate
FROM  {{ source('op_optimism', 'GovernanceToken_evt_DelegateChanged') }}
{% if is_incremental() %}
    WHERE evt_block_time >= DATE_TRUNC('day', NOW() - INTERVAL '7' DAY)
{% endif %}

