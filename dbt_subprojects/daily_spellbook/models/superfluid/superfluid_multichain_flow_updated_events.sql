{{ config(
    schema = 'superfluid_multichain',
    alias = 'flow_updated_events',
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
    varbinary_substring(topic2, 13) as sender,
    varbinary_substring(topic3, 13) as receiver,
    varbinary_to_int256(varbinary_substring(data, 1, 32)) as flow_rate
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
    ) AND topic0 = 0x57269d2ebcccecdcc0d9d2c0a0b80ead95f344e28ec20f50f709811f209d4e0e

LIMIT 100