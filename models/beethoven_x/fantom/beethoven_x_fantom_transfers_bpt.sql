{{
    config(
        schema = 'beethoven_x_fantom',
        alias = 'transfers_bpt',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_date', 'evt_tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.evt_block_time')]
    )
}}

    WITH registered_pools AS (
        SELECT
        DISTINCT poolAddress AS pool_address
        FROM
        {{ source('beethoven_x_fantom', 'Vault_evt_PoolRegistered') }}
    )

    SELECT DISTINCT * FROM (
        SELECT
            'fantom' AS blockchain,
            transfer.contract_address,
            transfer.evt_tx_hash,
            transfer.evt_index,
            transfer.evt_block_time,
            TRY_CAST(date_trunc('DAY', transfer.evt_block_time) AS date) AS block_date,
            TRY_CAST(date_trunc('MONTH', transfer.evt_block_time) AS date) AS block_month,
            transfer.evt_block_number,
            transfer."from",
            transfer.to,
            transfer.value
        FROM {{ source('erc20_fantom',  'evt_transfer') }} transfer
        INNER JOIN registered_pools p ON p.pool_address = transfer.contract_address
            {% if not is_incremental() %}
            WHERE transfer.evt_block_time >= TIMESTAMP '2021-08-26'
            {% endif %}
            {% if is_incremental() %}
            WHERE {{ incremental_predicate('evt_block_time') }}
            {% endif %} ) transfers