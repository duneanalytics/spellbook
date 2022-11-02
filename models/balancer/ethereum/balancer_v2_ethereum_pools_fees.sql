{{
    config(
        schema = 'balancer_v2_ethereum',
        alias='pools_fees',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_number', 'tx_hash', 'index'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "balancer_v2",
                                    \'["masquot", "jacektrocinski"]\') }}'
    ) 
}}

{% set event_signature = "0xa9ba3ffe0b6c366b81232caab38605a0699ad5398d6cce76f91ee809e322dafc" %}

WITH registered_pools AS (
    SELECT DISTINCT
        `poolAddress` AS pool_address
    FROM
        {{ source ('balancer_v2_ethereum', 'Vault_evt_PoolRegistered') }}
)
SELECT
    /*+ BROADCASTJOIN (registered_pools) */
    logs.contract_address,
    logs.tx_hash,
    logs.tx_index,
    logs.index,
    logs.block_time,
    logs.block_number,
    bytea2numeric_v2 (SUBSTRING(logs.data FROM 32 FOR 64)) * 1 AS swap_fee_percentage
FROM
    {{ source ('ethereum', 'logs') }}
    INNER JOIN registered_pools ON registered_pools.pool_address = logs.contract_address
        AND logs.topic1 = '{{ event_signature }}'
{% if is_incremental() %}
WHERE
    logs.block_time >= DATE_TRUNC('day', NOW() - interval '1 week')
{% endif %}
;

