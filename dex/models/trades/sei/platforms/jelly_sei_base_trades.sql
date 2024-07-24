{{
    config(
        schema = 'jelly_sei',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

SELECT * FROM (
        SELECT
            swaps.poolId,
            swaps.evt_tx_hash,
            swaps.evt_index,
            swaps.evt_block_number,
            bytearray_substring(swaps.poolId, 1, 20) AS contract_address,
            fees.swap_fee_percentage,
            ROW_NUMBER() OVER (PARTITION BY poolId, evt_tx_hash, evt_index ORDER BY block_number DESC, index DESC) AS rn
        FROM {{ source('jelly_swap_sei', 'Vault_evt_Swap') }} swaps
        LEFT JOIN {{ ref(project_decoded_as ~ '_' ~ blockchain ~ '_' ~ pools_fees) }} fees
            ON fees.contract_address = bytearray_substring(swaps.poolId, 1, 20)
            AND ARRAY[fees.block_number] || ARRAY[fees.index] < ARRAY[swaps.evt_block_number] || ARRAY[swaps.evt_index]
        {% if is_incremental() %}
        WHERE {{ incremental_predicate('swaps.evt_block_time') }}
        {% endif %}
    ) t
    WHERE t.rn = 1