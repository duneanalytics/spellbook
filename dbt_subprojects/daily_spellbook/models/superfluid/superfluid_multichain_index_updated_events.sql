{{ config(
    schema = 'superfluid_multichain',
    alias = 'index_updated_events',
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
    varbinary_substring(topic2, 13) AS publisher,
    varbinary_to_int256(topic3) AS index_id,
    varbinary_to_int256(varbinary_substring(data, 1, 32)) AS old_index_value,
    varbinary_to_int256(varbinary_substring(data, 33, 32)) AS new_index_value,
    varbinary_to_int256(varbinary_substring(data, 65, 32)) AS total_units_pending,
    varbinary_to_int256(varbinary_substring(data, 97, 32)) AS total_units_approved
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
    ) AND topic0 = 0x81e37f3d9f16cbf29a62d6a1c21d79b23ef29b54124ec44af43a50fffb9304f3
