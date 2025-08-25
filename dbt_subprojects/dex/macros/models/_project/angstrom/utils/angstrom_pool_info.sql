{% macro
    angstrom_pool_info(
        controller_v1_contract_addr,
        earliest_block,
        blockchain
    )
%}

WITH fee_events AS (
    SELECT 
        block_number,
        varbinary_to_integer(varbinary_substring(l.data, 31, 2))  AS tick_spacing,
        varbinary_to_integer(varbinary_substring(l.data, 62, 3)) AS bundle_fee,
        varbinary_to_integer(varbinary_substring(l.data, 94, 3)) AS unlocked_fee,
        varbinary_to_integer(varbinary_substring(l.data, 126, 3)) AS protocol_unlocked_fee,
        topic1,
        topic2,
        CONCAT(
            '0x',
            LOWER(
            to_hex(
                keccak(
                FROM_HEX(
                    CONCAT(
                    LPAD(LOWER(to_hex(varbinary_substring(l.topic1, 13, 20))), 64, '0'),
                    LPAD(LOWER(to_hex(varbinary_substring(l.topic2, 13, 20))), 64, '0'),
                    LPAD('800000', 64, '0'),
                    LPAD(
                        LOWER(
                        to_base(
                            bitwise_and(CAST(varbinary_to_integer(varbinary_substring(l.data, 31, 2)) AS BIGINT), 16777215),
                            16
                        )
                        ),
                        64,
                        '0'
                    ),
                    LPAD('0000000aa232009084bd71a5797d089aa4edfad4', 64, '0')
                    )
                )
                )
            )
            )
        ) AS pool_id
    FROM {{ source(blockchain, 'logs') }} AS l
    WHERE 
        block_number >= {{ earliest_block }} AND
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
    topic1,
    topic2,
    FROM_HEX(pool_id) AS pool_id
FROM latest_fees_per_pair
WHERE rn = 1 AND bundle_fee IS NOT NULL
ORDER BY block_number DESC, topic1, topic2


{% endmacro %}