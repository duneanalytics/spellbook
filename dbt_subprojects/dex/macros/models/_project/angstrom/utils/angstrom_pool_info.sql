{% macro
    angstrom_pool_info(
        angstrom_contract_addr,
        controller_v1_contract_addr,
        earliest_block,
        blockchain,
        controller_pool_configured_log_topic0
    )
%}

WITH fee_events AS (
    -- The ConfigurePool log scan is non-pushable (contract_address + topic0 varbinary equality)
    -- so each inlined copy re-scans the full ethereum.logs window. angstrom_pool_info is referenced
    -- across three base_trades lineage branches and the recursive/nested order macros expand it into
    -- 18 logs scan operators (~99% of the model's physical IO). For ethereum we read the shared
    -- pre-materialized staging model instead; the forward-fill below needs the FULL config history,
    -- so this read is intentionally NOT windowed by the incremental predicate. CUR2-2837.
    {%- if blockchain == 'ethereum' %}
    SELECT
        block_number,
        tick_spacing,
        bundle_fee,
        unlocked_fee,
        protocol_unlocked_fee,
        topic1,
        topic2,
        pool_id
    FROM {{ ref('angstrom_ethereum_fee_events') }}
    {%- else %}
    SELECT
        block_number,
        tick_spacing,
        bundle_fee,
        unlocked_fee,
        protocol_unlocked_fee,
        topic1,
        topic2,
        pool_id
    FROM (
        {{ angstrom_fee_events_raw(angstrom_contract_addr, controller_v1_contract_addr, earliest_block, blockchain, controller_pool_configured_log_topic0) }}
    )
    {%- endif %}
),
block_range AS (
    SELECT number AS block_number
    FROM {{ source(blockchain, 'blocks') }}
    WHERE number >= (SELECT MIN(block_number) FROM fee_events)
),
topic_pairs AS (
    SELECT DISTINCT topic1, topic2
    FROM fee_events
),
block_topic_combinations AS (
    SELECT 
        br.block_number,
        tp.topic1,
        tp.topic2
    FROM block_range br
    CROSS JOIN topic_pairs tp
),
latest_fees_per_pair AS (
    SELECT 
        btc.block_number,
        btc.topic1,
        btc.topic2,
        fe.pool_id,
        fe.tick_spacing,
        fe.bundle_fee,
        fe.unlocked_fee,
        fe.protocol_unlocked_fee,
        ROW_NUMBER() OVER (
            PARTITION BY btc.block_number, btc.topic1, btc.topic2 
            ORDER BY fe.block_number DESC
        ) AS rn
    FROM block_topic_combinations btc
    LEFT JOIN fee_events fe 
        ON fe.topic1 = btc.topic1 
        AND fe.topic2 = btc.topic2
        AND fe.block_number <= btc.block_number
)
SELECT 
    block_number,
    bundle_fee,
    unlocked_fee,
    protocol_unlocked_fee,
    varbinary_substring(topic1, 13, 20) AS token0,
    varbinary_substring(topic2, 13, 20) AS token1,
    FROM_HEX(pool_id) AS pool_id
FROM latest_fees_per_pair
WHERE rn = 1 AND bundle_fee IS NOT NULL
ORDER BY block_number DESC, topic1, topic2


{% endmacro %}