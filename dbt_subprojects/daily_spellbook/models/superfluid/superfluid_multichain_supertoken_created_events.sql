{{ config(
    schema = 'superfluid_multichain',
    alias = 'supertoken_created_events',
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
    (CASE
        WHEN topic0 = 0xb52c6d9d122e8c07769b96d7bb14e66db58ee03fdebaaa2f92547e9c7ef0e65f THEN 'wrapper_token'
        WHEN topic0 = 0x437790724a6e97b75d23117f28cdd4b1beeafc34f7a0911ef256e9334f4369a5 THEN 'supertoken'
    END) AS "token_type",
    COALESCE(varbinary_substring(topic1, 13), varbinary_substring("data", 13)) AS "token"
FROM {{ ref('evms_logs') }}
WHERE blockchain IN (
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
) AND (
    topic0 = 0xb52c6d9d122e8c07769b96d7bb14e66db58ee03fdebaaa2f92547e9c7ef0e65f OR
    topic0 = 0x437790724a6e97b75d23117f28cdd4b1beeafc34f7a0911ef256e9334f4369a5
)