{{ config(
    schema = 'gas_solana',
    alias = 'fees',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'delete+insert',
    unique_key = ['block_date', 'block_slot', 'tx_index', 'tx_type']
) }}

WITH combined_fees AS (
    SELECT
        'normal' as tx_type,
        *
    FROM {{ ref('gas_solana_tx_fees') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('block_date') }}
    {% else %}
    WHERE block_date >= DATE_TRUNC('DAY', CURRENT_DATE) - INTERVAL '30' DAY
    {% endif %}

    UNION ALL

    SELECT
        'vote' as tx_type,
        f.*
    FROM {{ ref('gas_solana_tx_fees') }} f
    JOIN {{ ref('solana_vote_fees') }} vf
        ON f.tx_hash = vf.tx_hash
        AND f.block_slot = vf.block_slot
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('f.block_date') }}
    {% else %}
    WHERE f.block_date >= DATE_TRUNC('DAY', CURRENT_DATE) - INTERVAL '30' DAY
    {% endif %}
)

SELECT *
FROM combined_fees
