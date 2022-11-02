{{ config (schema = 'balancer_v2_ethereum', alias = 'pool_fees', post_hook = '{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "balancer_v2",
                                    \'["masquot", "jacektrocinski"]\') }}') }}

{% set event_signature = "0xba12222222228d8ba445958a75a0704d566bf2c8" %}

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
    logs.block_time,
    logs.block_number,
    bytea2numeric_v2 (SUBSTRING(logs.data FROM 32 FOR 64)) * 1 AS swap_fee_percentage
FROM
    {{ source ('ethereum', 'logs') }}
    INNER JOIN registered_pools ON registered_pools.pool_address = logs.contract_address
        AND logs.topic1 = '{{ event_signature }}';

