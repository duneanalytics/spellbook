{{  config(
        schema = 'zeroex_bnb',
        alias = 'fills_uni_v2_pair_creation',
        materialized='incremental',
        unique_key = ['pair', 'makerToken', 'takerToken'],
        file_format ='delta',
        incremental_strategy='merge'
    )
}}

{% set zeroex_v3_start_date = '2019-12-01' %}

-- This model exists as a performance optimization so that we don't do a full scan on bnb.logs

SELECT
    block_time,
    pair,
    makerToken,
    takerToken
 FROM (
SELECT
    block_time,
    bytearray_substring(data,13,20) as pair,
    bytearray_substring(topic1, 13, 20) AS makerToken,
    bytearray_substring(topic2, 13, 20) AS takerToken,
    row_number() over (partition by bytearray_substring(creation.data, 13, 20) order by block_time ) rn
FROM {{ source('bnb', 'logs') }} creation
WHERE creation.topic0 = 0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9  -- all the uni v2 pair creation event
    {% if is_incremental() %}
    AND {{ incremental_predicate('block_time') }}
    {% else %}
    AND block_time >= TIMESTAMP '{{zeroex_v3_start_date}}'
    {% endif %}
)
WHERE rn = 1