{{ config(
    schema = 'solana_utils',
    alias = 'block_leaders',
    materialized = 'incremental',
    partition_by = ['month'],
    file_format = 'delta',
    unique_key = ['slot'],
    incremental_strategy = 'merge',
    post_hook='{{ expose_spells(\'["solana"]\',
                                    "sector",
                                    "solana_utils",
                                    \'["0xBoxer"]\') }}'
) }}

SELECT 
    date_trunc('month', b.time) as month,
    b.time,
    b.slot,
    b.height,
    b.hash,
    b.total_transactions,
    b.successful_transactions,
    b.failed_transactions,
    b.total_vote_transactions,
    b.total_non_vote_transactions,
    b.successful_vote_transactions,
    b.successful_non_vote_transactions,
    b.failed_vote_transactions,
    b.failed_non_vote_transactions,
    b.parent_slot,
    b.previous_block_hash,
    r.recipient AS leader
FROM {{ source('solana', 'blocks') }} b 
LEFT JOIN {{ source('solana', 'rewards') }} r 
    ON b.slot = r.block_slot 
    AND r.reward_type = 'Fee'

--not all slots have leaders, but slots without leaders don't have transactions so can be disregarded.

{% if is_incremental() %}
    AND {{incremental_predicate('b.time')}}
{% endif %}
