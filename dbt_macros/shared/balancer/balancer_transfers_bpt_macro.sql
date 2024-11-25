{% macro 
    balancer_v2_compatible_transfers_bpt_macro(
        blockchain, version, project_decoded_as 
    ) 
%}

    WITH registered_pools AS (
        SELECT
        DISTINCT poolAddress AS pool_address
        FROM
        {{ source(project_decoded_as + '_' + blockchain, 'Vault_evt_PoolRegistered') }}
    )

    SELECT DISTINCT * FROM (
        SELECT
            '{{blockchain}}' AS blockchain,
            '{{version}}' AS version,
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
        FROM {{ source('erc20_' + blockchain, 'evt_transfer') }} transfer
        INNER JOIN registered_pools p ON p.pool_address = transfer.contract_address
            {% if not is_incremental() %}
            WHERE transfer.evt_block_time >= TIMESTAMP '2021-08-26'
            {% endif %}
            {% if is_incremental() %}
            WHERE {{ incremental_predicate('evt_block_time') }}
            {% endif %} ) transfers


 {% endmacro %}

 {% macro 
    balancer_v3_compatible_transfers_bpt_macro(
        blockchain, version, project_decoded_as 
    ) 
%}

    WITH registered_pools AS (
        SELECT
        DISTINCT pool AS pool_address
        FROM
        {{ source(project_decoded_as + '_' + blockchain, 'Vault_evt_PoolRegistered') }}
    )

    SELECT DISTINCT * FROM (
        SELECT
            '{{blockchain}}' AS blockchain,
            '{{version}}' AS version,
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
        FROM {{ source('erc20_' + blockchain, 'evt_transfer') }} transfer
        INNER JOIN registered_pools p ON p.pool_address = transfer.contract_address
            {% if not is_incremental() %}
            WHERE transfer.evt_block_time >= TIMESTAMP '2024-12-01'
            {% endif %}
            {% if is_incremental() %}
            WHERE {{ incremental_predicate('evt_block_time') }}
            {% endif %} ) transfers


 {% endmacro %}