{{
    config(
        schema = 'balancer_v2_arbitrum',
        alias = 'transfers_bpt',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_date', 'evt_tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["arbitrum"]\',
                                    "project",
                                    "balancer_v2",
                                    \'["stefenon", "thetroyharris"]\') }}'
    )
}}

{% set event_signature = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' %}
{% set project_start_date = '2021-08-26' %}

WITH registered_pools AS (
    SELECT
      DISTINCT poolAddress AS pool_address
    FROM
      {{ source('balancer_v2_arbitrum', 'Vault_evt_PoolRegistered') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= DATE_TRUNC('day', NOW() - interval '7' day)
    {% endif %}
  )

SELECT DISTINCT * FROM (
    SELECT
        'arbitrum' AS blockchain,
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
    FROM {{ source('erc20_arbitrum', 'evt_Transfer') }} transfer
    INNER JOIN registered_pools p ON p.pool_address = transfer.contract_address
        {% if not is_incremental() %}
        WHERE transfer.block_time >= TIMESTAMP '{{ project_start_date }}'
        {% endif %}
        {% if is_incremental() %}
        WHERE transfer.block_time >= DATE_TRUNC('day', NOW() - interval '7' day)
        {% endif %} ) transfers
