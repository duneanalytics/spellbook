{{ config(
    schema = 'superfluid_multichain',
    alias = 'instant_distribution_updated_events',
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
    varbinary_to_int256(varbinary_substring(data, 65, 32)) as actual_amount
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
    ) AND topic0 = 0x6e8348961b299f365797c30cd18a91284b046858689eeb6150a5ba432fe6583e

LIMIT 100