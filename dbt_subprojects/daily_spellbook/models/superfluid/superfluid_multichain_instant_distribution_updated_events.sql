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
    tx_hash,
    index,
    contract_address,
    varbinary_substring(topic1, 13) AS token,
    varbinary_substring(topic2, 13) AS pool,
    varbinary_substring(topic3, 13) AS distributor,
    varbinary_to_int256(varbinary_substring(data, 65, 32)) AS actual_amount
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
    ) AND topic0 = 0x6e8348961b299f365797c30cd18a91284b046858689eeb6150a5ba432fe6583e
