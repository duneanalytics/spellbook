{{ config(
    schema = 'superfluid_arbitrum',
    alias = 'flow_distribution_updated_events',
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
        varbinary_substring(topic1, 13) as token,
        varbinary_substring(topic2, 13) as pool,
        varbinary_substring(topic3, 13) as distributor,
        varbinary_substring(data, 13, 20) as operator,
        varbinary_to_int256(varbinary_substring(data, 33, 32)) as oldFlowRate,
        varbinary_to_int256(varbinary_substring(data, 65, 32)) as newDistributorToPoolFlowRate,
        varbinary_to_int256(varbinary_substring(data, 97, 32)) as newTotalDistributionFlowRate,
        varbinary_substring(data, 141, 20) as adjustmentFlowRecipient,
        varbinary_to_int256(varbinary_substring(data, 161, 32)) as adjustmentFlowRate
    FROM 
        {{ source('arbitrum', 'logs') }}
    WHERE 
        topic0 = 0xc4bd0e4bfe3a83cbd5a7a71bb33bcb6bed9e0c24d710f3fb51c85caf1cd3af36
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
    oldFlowRate,
    newDistributorToPoolFlowRate,
    newTotalDistributionFlowRate,
    adjustmentFlowRecipient,
    adjustmentFlowRate
FROM data