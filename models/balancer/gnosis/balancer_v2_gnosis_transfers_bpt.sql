{{
    config(
        schema='balancer_v2_gnosis',
        tags = ['dunesql'],
        alias = alias('transfers_bpt'),
        partition_by = ['block_date'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_date', 'evt_tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["gnosis"]\',
                                    "project",
                                    "balancer_v2",
                                    \'["stefenon", "thetroyharris"]\') }}'
    )
}}

{% set project_start_date = '2022-11-02' %}

WITH registered_pools AS (
    SELECT
      DISTINCT poolAddress AS pool_address
    FROM
      {{ source('balancer_v2_gnosis', 'Vault_evt_PoolRegistered') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= DATE_TRUNC('day', NOW() - interval '1 week')
    {% endif %}
  )

SELECT DISTINCT * FROM (
    SELECT
        'gnosis' AS blockchain,
        logs.contract_address,
        logs.tx_hash AS evt_tx_hash,
        logs.index AS evt_index,
        logs.block_time AS evt_block_time,
        TRY_CAST(date_trunc('DAY', logs.block_time) AS date) AS block_date,
        logs.block_number AS evt_block_number,
        bytearray_substring(topic1, 13) AS "from",
        bytearray_substring(topic2, 13) AS to,
        bytearray_to_uint256(logs.data) AS value
    FROM {{ source('gnosis', 'logs') }} logs
    INNER JOIN registered_pools p ON p.pool_address = logs.contract_address
    WHERE logs.topic0 = 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef
        {% if not is_incremental() %}
        AND logs.block_time >= '{{ project_start_date }}'
        {% endif %}
        {% if is_incremental() %}
        AND logs.block_time >= DATE_TRUNC('day', NOW() - interval '1 week')
        {% endif %} ) transfers
