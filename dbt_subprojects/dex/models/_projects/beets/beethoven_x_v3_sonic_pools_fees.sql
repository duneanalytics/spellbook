{{
    config(
        schema = 'beethoven_x_v3_sonic',
        alias = 'pools_fees',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_number', 'tx_hash', 'index'],
        post_hook='{{ expose_spells(\'["sonic"]\',
                                    "project",
                                    "beethoven_x",
                                    \'["viniabussafi"]\') }}'
    ) 
}}

    SELECT
        'sonic' AS blockchain,
        '3' AS version, 
        pool AS contract_address,
        evt_tx_hash AS tx_hash,
        evt_index AS tx_index,
        evt_index AS index,
        evt_block_time AS block_time,
        evt_block_number AS block_number,
        swapFeePercentage AS swap_fee_percentage
    FROM {{ source ('beethoven_x_v3_sonic', 'Vault_evt_SwapFeePercentageChanged') }}
    WHERE 1 = 1 
    {% if is_incremental() %}
    AND {{ incremental_predicate('evt_block_time') }}
    {% endif %}