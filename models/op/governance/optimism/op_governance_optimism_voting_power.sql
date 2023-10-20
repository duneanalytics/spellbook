{{ config(
         tags=['dunesql']
        , schema = 'op_governance_optimism'
        , alias = alias('voting_power')
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
delegate,
CAST(newBalance AS DOUBLE)/1e18 AS newBalance, 
CAST(previousBalance AS DOUBLE)/1e18 AS previousBalance,
CAST(newBalance AS DOUBLE)/1e18 - CAST(previousBalance AS DOUBLE)/1e18 AS power_diff
FROM {{ source('op_optimism', 'GovernanceToken_evt_DelegateVotesChanged') }} 
WHERE CAST(evt_block_time AS DATE) >= DATE'2022-05-26'
{% if is_incremental() %}
    AND {{ incremental_predicate('evt_block_time') }}
{% endif %}
