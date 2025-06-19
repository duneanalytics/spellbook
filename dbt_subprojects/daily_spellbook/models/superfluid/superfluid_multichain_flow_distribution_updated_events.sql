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
    block_number,
    block_hash,
    contract_address,
    tx_hash,
    index,
    varbinary_substring(topic1, 13) as token,
    varbinary_substring(topic2, 13) as pool,
    varbinary_substring(topic3, 13) as distributor,
    varbinary_to_int256(varbinary_substring(data, 97, 32)) as new_total_distribution_flow_rate
FROM {{ source('evms','logs') }}
WHERE 
    blockchain IN (
        'gnosis',
        'polygon',
        'optimism',
        'arbitrum',
        'avalanche_c',
        'bnb',
        'ethereum',
        'celo',
        'base',
        'scroll'
    ) AND topic0 = 0xc4bd0e4bfe3a83cbd5a7a71bb33bcb6bed9e0c24d710f3fb51c85caf1cd3af36

LIMIT 100
