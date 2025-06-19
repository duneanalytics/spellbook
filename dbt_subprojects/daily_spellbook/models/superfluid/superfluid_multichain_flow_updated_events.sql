{{ config(
    schema = 'superfluid_multichain',
    alias = 'flow_updated_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['blockchain','tx_hash','index'],
    )
}}

WITH flow_updated_events AS (
    SELECT
        blockchain,
        block_time,
        block_date,
        block_number,
        tx_hash,
        index,
        contract_address,
        varbinary_substring(topic1, 13) AS token,
        varbinary_substring(topic2, 13) AS sender,
        varbinary_substring(topic3, 13) AS receiver,
        varbinary_to_int256(varbinary_substring(data, 1, 32)) AS flow_rate
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
        ) AND topic0 = 0x57269d2ebcccecdcc0d9d2c0a0b80ead95f344e28ec20f50f709811f209d4e0e
)

/**
 * to create the stream_periods from the flow updated events, simply find the end of the period by using the next
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
    sender,
    receiver,
    flow_rate,
    block_time AS start_block_time,
    COALESCE(
        LEAD(block_time) OVER (PARTITION BY blockchain, token, sender, receiver ORDER BY block_number, index),
        current_timestamp
    ) AS end_block_time
FROM flow_updated_events