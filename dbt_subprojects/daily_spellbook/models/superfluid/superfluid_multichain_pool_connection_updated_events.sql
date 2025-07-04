{{ config(
    schema = 'superfluid_multichain',
    alias = 'pool_connection_updated_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['blockchain','tx_hash','index'],
    )
}}

WITH pool_connection_updated_events AS (
    SELECT
        blockchain,
        block_time,
        block_date,
        block_number,
        tx_hash,
        index,
        contract_address,
        varbinary_substring(topic1, 13) AS token,
        varbinary_substring(topic2, 13) AS pool,
        varbinary_substring(topic3, 13) AS account,
        if(varbinary_to_uint256(varbinary_substring(data, 1, 32)) = 1, true, false) AS connected
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
        ) AND
        topic0 = 0xf357a2e6919da4efedb0301baf1caaed2bca70b115b913f9add41bbefc75c9b3
)

/**
 * to create the stream_periods from the pool connection updated events, simply find the end of the period by using the next
 * (LEAD()) event (ordered by blocknumber and index - in case there are multiple events in the same block) for each stream.
 */

SELECT
    blockchain,
    block_time,
    block_date,
    tx_hash,
    index,
    contract_address,
    token,
    pool,
    account,
    connected,
    block_time AS start_block_time,
    LEAD(block_time) OVER (PARTITION BY blockchain, token, pool, account ORDER BY block_number, index) AS end_block_time_or_null,
    COALESCE(
        LEAD(block_time) OVER (PARTITION BY blockchain, token, pool, account ORDER BY block_number, index),
        current_timestamp
    ) AS end_block_time
FROM pool_connection_updated_events