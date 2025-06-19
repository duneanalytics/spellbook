{{ config(
    schema = 'superfluid_multichain',
    alias = 'token_upgraded_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['blockchain','tx_hash','index'],
    )
}}

SELECT
    blockchain,
    evt_block_time AS block_time,
    evt_block_date AS block_date,
    evt_tx_hash AS tx_hash,
    evt_index AS index,
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
    ) AND topic0 = 0x25ca84076773b0455db53621c459ddc84fe40840e4932a62706a032566f399df
