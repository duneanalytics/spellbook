{{ config(
    schema = 'superfluid_multichain',
    alias = 'flow_distribution_updated_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['blockchain','tx_hash','index'],
    )
}}

SELECT
    blockchain,
    block_time,
    block_date,
    tx_hash,
    index,
    contract_address,
    varbinary_substring(topic1, 13) AS token,
    varbinary_substring(topic2, 13) AS pool,
    varbinary_substring(topic3, 13) AS distributor,
    varbinary_substring(data, 13, 20) AS operator,
    varbinary_to_int256(varbinary_substring(data, 33, 32)) AS old_flow_rate,
    varbinary_to_int256(varbinary_substring(data, 65, 32)) AS new_distributor_to_pool_flow_rate,
    varbinary_to_int256(varbinary_substring(data, 97, 32)) AS new_total_distribution_flow_rate,
    varbinary_substring(data, 141, 20) AS adjustment_flow_recipient,
    varbinary_to_int256(varbinary_substring(data, 161, 32)) AS adjustment_flow_rate
FROM {{ ref('evms_logs') }}
WHERE 
    blockchain IN (
        'arbitrum',
        'avalanche_c',
        'base',
        'bnb',
        'celo',
        'ethereum',
        'gnosis',
        'optimism',
        'polygon',
        'scroll'
    ) AND topic0 = 0xc4bd0e4bfe3a83cbd5a7a71bb33bcb6bed9e0c24d710f3fb51c85caf1cd3af36
