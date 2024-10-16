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
, s.account_to AS address
, MIN_BY(s.account_from, s.call_block_slot) AS first_funded_by
, MIN(s.call_block_time) AS block_time
, MIN(s.call_block_slot) AS block_slot
, MIN_BY(s.call_tx_id, (s.call_block_slot, s.call_tx_index)) AS tx_id
, MIN_BY(s.call_tx_index, (s.call_block_slot, s.call_tx_index)) AS tx_index
FROM {{ source('system_program_solana', 'system_program_call_Transfer') }} s
{% if is_incremental() %}
LEFT JOIN {{this}} ffb ON s.account_to = ffb.address WHERE ffb.address IS NULL
{% else %}
WHERE 1 = 1
{% endif %}
{% if is_incremental() %}
AND {{incremental_predicate('s.call_block_time')}}
{% endif %}
GROUP BY 1, 2