{% macro
    angstrom_fee_events_raw(
        angstrom_contract_addr,
        controller_v1_contract_addr,
        earliest_block,
        blockchain,
        controller_pool_configured_log_topic0
    )
%}

SELECT
    block_number,
    block_time,
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
                LPAD(substr(CAST({{ angstrom_contract_addr }} AS VARCHAR), 3), 64, '0')
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
    topic0 = {{ controller_pool_configured_log_topic0 }}

{% endmacro %}
