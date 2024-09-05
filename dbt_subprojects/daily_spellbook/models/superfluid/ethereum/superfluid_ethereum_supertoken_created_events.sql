{{ config(
    schema = 'superfluid_ethereum',
    alias = 'supertoken_created_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge'
    )
}}

{% set project_start_block_number = '15870000' %}

WITH wrapper_tokens as (
    SELECT
        block_time,
        block_number,
        tx_hash,
        index,
        varbinary_substring(topic1, 13) as address
    FROM 
        {{ source('ethereum', 'logs') }}
    WHERE
        topic0 = 0xb52c6d9d122e8c07769b96d7bb14e66db58ee03fdebaaa2f92547e9c7ef0e65f
        {% if is_incremental() %}
        AND 
        {{ incremental_predicate('block_time') }}
        {% else %}
        AND block_number >= INTEGER '{{project_start_block_number}}'
        {% endif %}
), 
custom_tokens as (
    SELECT
        block_time,
        block_number,
        tx_hash,
        index,
        varbinary_substring(topic1, 13) as address
    FROM 
        {{ source('ethereum', 'logs') }}
    WHERE
        topic0 = 0x437790724a6e97b75d23117f28cdd4b1beeafc34f7a0911ef256e9334f4369a5
        {% if is_incremental() %}
        AND 
        {{ incremental_predicate('block_time') }}
        {% else %}
        AND block_number >= INTEGER '{{project_start_block_number}}'
        {% endif %}
),
combined_tokens as (
    SELECT * FROM wrapper_tokens
    UNION
    SELECT * FROM custom_tokens
)
SELECT
    block_time,
    block_number,
    tx_hash,
    index,
    address
FROM combined_tokens
