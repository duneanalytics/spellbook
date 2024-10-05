{{ config(
    schema = 'superfluid_polygon',
    alias = 'flow_updated_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge'
    )
}}

{% set project_start_block_number = '11650500' %}

WITH data as (
    SELECT
        block_time, 
        block_number,
        tx_hash,
        index,
        varbinary_substring(topic1, 13) as token,
        varbinary_substring(topic2, 13) as sender,
        varbinary_substring(topic3, 13) as receiver,
        varbinary_to_int256(varbinary_substring(data, 1, 32)) as flowRate
    FROM 
        {{ source('polygon', 'logs') }}
    WHERE 
        topic0 = 0x57269d2ebcccecdcc0d9d2c0a0b80ead95f344e28ec20f50f709811f209d4e0e
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
    sender,
    receiver,
    flowRate
FROM data