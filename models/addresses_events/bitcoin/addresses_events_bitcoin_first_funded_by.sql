{{ config(
    schema = 'addresses_events_bitcoin'
    
    , alias = 'first_funded_by'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'append'
    , unique_key = ['address']
    )
}}

WITH first_appearance AS (
    SELECT o.address
    , MIN(o.block_time) AS block_time
    , CAST(MIN(o.block_height) AS BIGINT) AS block_height
    , MIN_BY(o.tx_id, o.block_height) AS tx_id
    FROM {{ source('bitcoin', 'outputs') }} o
    {% if is_incremental() %}
    LEFT JOIN {{this}} ffb ON o.address = ffb.address WHERE ffb.address IS NULL
    {% else %}
    WHERE 1 = 1
    {% endif %}
    {% if is_incremental() %}
    AND {{incremental_predicate('o.block_time')}}
    {% endif %}
    GROUP BY 1
    )

SELECT 'bitcoin' AS blockchain
, fa.address
, array_agg(i.address) AS first_funded_by
, fa.block_time
, fa.block_height
, fa.tx_id
FROM first_appearance fa 
INNER JOIN {{ source('bitcoin', 'inputs') }} i ON fa.block_height=i.block_height
    AND fa.tx_id=i.tx_id
GROUP BY 1, 2, 4, 5, 6