{{ config(
    schema = 'addresses_events_solana'
    
    , alias = 'first_funded_by'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'append'
    , unique_key = ['address']
    )
}}

SELECT 'solana' AS blockchain
, account_to AS address
, MIN_BY(account_from, call_block_slot) AS first_funded_by
, MIN(call_block_time) AS block_time
, MIN(call_block_slot) AS block_slot
, MIN_BY(call_tx_id, (call_block_slot, call_tx_index)) AS tx_id
, MIN_BY(call_tx_index, (call_block_slot, call_tx_index)) AS tx_index
FROM {{ source('system_program_solana', 'system_program_call_Transfer') }} s
{% if is_incremental() %}
LEFT JOIN {{this}} ffb ON s.account_to = ffb.account_to WHERE ffb.account_to IS NULL
{% else %}
{% if is_incremental() %}
WHERE {{incremental_predicate('call_block_time')}}
{% endif %}
GROUP BY 1, 2