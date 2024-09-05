{{ config(
    schema = 'superfluid_ethereum',
    alias = 'index_updated_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge'
    )
}}

{% set project_start_block_number = '15870000' %}

WITH data as (
    SELECT
        block_time,
        block_number,
        tx_hash,
        index,
        varbinary_substring(topic1, 13) as token,
        varbinary_substring(topic2, 13) as publisher,
        varbinary_to_int256(topic3) as indexId,
        varbinary_to_int256(varbinary_substring(data, 1, 32)) as oldIndexValue,
        varbinary_to_int256(varbinary_substring(data, 33, 32)) as newIndexValue,
        varbinary_to_int256(varbinary_substring(data, 65, 32)) as totalUnitsPending,
        varbinary_to_int256(varbinary_substring(data, 97, 32)) as totalUnitsApproved
    FROM 
        {{ source('ethereum', 'logs') }}
    WHERE 
        topic0 = 0x81e37f3d9f16cbf29a62d6a1c21d79b23ef29b54124ec44af43a50fffb9304f3
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
    publisher,
    indexId,
    oldIndexValue,
    newIndexValue,
    totalUnitsPending,
    totalUnitsApproved
FROM data