{{ config(
    schema = 'superfluid_arbitrum',
    alias = 'token_upgraded_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge'
    )
}}

{% set project_start_block_number = '7600000' %}

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
        {{ source('arbitrum', 'logs') }}
    WHERE 
        topic0 = 0x25ca84076773b0455db53621c459ddc84fe40840e4932a62706a032566f399df
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