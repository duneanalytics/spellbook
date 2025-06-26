{{ config(
    schema = 'superfluid_multichain',
    alias = 'token_downgraded_events',
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
    contract_address AS token,
    bytearray_substring(topic1, 13, 20) AS account,
    bytearray_to_int256(data) AS amount
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
    ) AND topic0 = 0x3bc27981aebbb57f9247dc00fde9d6cd91e4b230083fec3238fedbcba1f9ab3d
