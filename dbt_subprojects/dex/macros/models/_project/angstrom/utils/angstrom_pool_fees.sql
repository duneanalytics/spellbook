{% macro
    angstrom_pool_fees(
        controller_v1_contract_addr, 
        blockchain
    )
%}

-- maybe use abi for log??

WITH fee_events AS (
    SELECT 
        block_number,
        varbinary_to_integer(varbinary_substring(l.data, 62, 3)) AS bundle_fee,
        varbinary_to_integer(varbinary_substring(l.data, 94, 3)) AS unlocked_fee,
        varbinary_to_integer(varbinary_substring(l.data, 126, 3)) AS protocol_unlocked_fee,
        topic1,
        topic2
    FROM {{ source(blockchain, 'logs') }} AS l
    WHERE 
        contract_address = {{ controller_v1_contract_addr }} AND 
        topic0 = 0xf325a037d71efc98bc41dc5257edefd43a1d1162e206373e53af271a7a3224e9
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
    topic1,
    topic2
FROM latest_fees_per_pair
WHERE rn = 1 AND bundle_fee IS NOT NULL
ORDER BY block_number DESC, topic1, topic2


{% endmacro %}