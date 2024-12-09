{{
    config(
        schema = 'balancer_v3_gnosis',
        alias = 'pools_fees',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.evt_block_time')],
        unique_key = ['block_number', 'tx_hash', 'index']
    )
}}

SELECT
    'gnosis' AS blockchain,
    '3' AS version, 
    pool AS contract_address,
    evt_tx_hash AS tx_hash,
    evt_tx_index AS tx_index,
    evt_index AS index,
    evt_block_time AS block_time,
    evt_block_number AS block_number,
    swapFeePercentage AS swap_fee_percentage
FROM {{ source ('balancer_v3_gnosis', 'Vault_evt_SwapFeePercentageChanged ') }}
WHERE 1 = 1 
{% if is_incremental() %}
AND {{ incremental_predicate('evt_block_time') }}
{% endif %}

