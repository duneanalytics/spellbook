{{ config(
    schema = 'superfluid_multichain',
    alias = 'flow_distribution_updated_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['blockchain','tx_hash','index'],
    )
}}

WITH flow_distribution_updated_events AS (
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
        varbinary_substring(topic3, 13) AS distributor,
        varbinary_substring(data, 13, 20) AS operator,
        varbinary_to_int256(varbinary_substring(data, 33, 32)) AS old_flow_rate,
        varbinary_to_int256(varbinary_substring(data, 65, 32)) AS new_distributor_to_pool_flow_rate,
        varbinary_to_int256(varbinary_substring(data, 97, 32)) AS new_total_distribution_flow_rate,
        varbinary_substring(data, 141, 20) AS adjustment_flow_recipient,
        varbinary_to_int256(varbinary_substring(data, 161, 32)) AS adjustment_flow_rate
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
        ) AND topic0 = 0xc4bd0e4bfe3a83cbd5a7a71bb33bcb6bed9e0c24d710f3fb51c85caf1cd3af36
)

/**
 * to create the stream_periods from the flow distribution updated events, simply find the end of the period by using the next
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
    distributor,
    operator,
    old_flow_rate,
    new_distributor_to_pool_flow_rate,
    new_total_distribution_flow_rate,
    adjustment_flow_recipient,
    adjustment_flow_rate
    block_time AS start_block_time,
    COALESCE(
        LEAD(block_time) OVER (PARTITION BY blockchain, token, distributor, pool ORDER BY block_number, index),
        current_timestamp
    ) AS end_block_time
FROM flow_distribution_updated_events