{{ config(
    schema = 'superfluid_celo',
    alias = 'token_downgraded_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge'
    )
}}

{% set project_start_block_number = '16393000' %}

WITH data as (
    SELECT
        block_time,
        block_number,
        tx_hash,
        index,
        contract_address as token,
        bytearray_substring(topic1, 13, 20) as account,
        bytearray_to_int256(data) as amount
    FROM 
        {{ source('celo', 'logs') }}
    WHERE 
        topic0 = 0x3bc27981aebbb57f9247dc00fde9d6cd91e4b230083fec3238fedbcba1f9ab3d
        {% if is_incremental() %}
        AND 
        {{ incremental_predicate('block_time') }}
        {% else %}
        AND block_number >= INTEGER '{{project_start_block_number}}'
        {% endif %}
)

SELECT 
    block_time,
    block_number,
    tx_hash,
    index,
    token,
    account,
    amount
FROM data