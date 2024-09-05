{{ config(
    schema = 'superfluid_optimism',
    alias = 'instant_distribution_updated_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge'
    )
}}

{% set project_start_block_number = '4300000' %}

WITH data as (
    SELECT
        block_time,
        block_number,
        tx_hash,
        index,
        varbinary_substring(topic1, 13) as token,
        varbinary_substring(topic2, 13) as pool,
        varbinary_substring(topic3, 13) as distributor,
        varbinary_substring(data, 13, 20) as operator,
        varbinary_to_int256(varbinary_substring(data, 33, 32)) as requestedAmount,
        varbinary_to_int256(varbinary_substring(data, 65, 32)) as actualAmount
    FROM 
        {{ source('optimism', 'logs') }}
    WHERE 
        topic0 = 0x6e8348961b299f365797c30cd18a91284b046858689eeb6150a5ba432fe6583e
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
    pool,
    distributor,
    operator,
    requestedAmount,
    actualAmount
FROM data