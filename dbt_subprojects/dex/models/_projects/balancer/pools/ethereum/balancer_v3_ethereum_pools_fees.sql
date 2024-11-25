{{
    config(
        schema = 'balancer_v3_ethereum',
        alias = 'pools_fees',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.evt_block_time')],
        unique_key = ['block_number', 'tx_hash', 'index']
    )
}}

SELECT
    'ethereum' AS blockchain,
    '3' AS version, 
    pool AS pool_address,
    evt_tx_hash,
    evt_tx_index,
    evt_index,
    evt_block_time,
    evt_block_number,
    swapFeePercentage AS swap_fee_percentage
FROM {{ source ('balancer_v2_ethereum', 'Vault_evt_SwapFeePercentageChanged ') }}
WHERE 1 = 1 
{% if is_incremental() %}
AND {{ incremental_predicate('evt_block_time') }}
{% endif %}

