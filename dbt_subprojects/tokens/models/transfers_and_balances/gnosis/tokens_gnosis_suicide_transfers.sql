{{config(
    schema = 'tokens_gnosis',
    alias = 'suicide_transfers',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['block_date','unique_key'],
)
}}

WITH 

suicide AS (
    SELECT 
         cast(date_trunc('month', block_time) as date) AS block_month
        , cast(date_trunc('day', block_time) as date) AS block_date
        , block_time
        , block_number
        , tx_hash
        , tx_index
        , trace_address
        , tx_from
        , tx_to
        , address 
        , refund_address 
    FROM 
        {{ source('gnosis', 'traces') }}
    WHERE
        type = 'suicide'
        AND
        success
),

suicide_events AS (
    SELECT
        *
        ,ROW_NUMBER() OVER (PARTITION BY address ORDER BY block_time) AS event_sequence
        ,LAG(block_time) OVER (PARTITION BY address ORDER BY block_time) AS previous_block_time
    FROM
        suicide
),

tokens_gnosis_base_without_suicide_transfers AS (
    SELECT
        address
        ,event_sequence
        ,SUM(amount_raw) AS amount_raw
    FROM (
        SELECT
            t2.address 
            ,-t1.amount_raw AS amount_raw
            ,t2.event_sequence
        FROM    
            {{ ref('tokens_gnosis_base_without_suicide_transfers') }} t1
        INNER JOIN
            suicide_events t2
            ON 
            t2.address = t1."from"
        WHERE  
            t1.token_standard = 'native'
            AND 
            (t1.block_time > t2.previous_block_time OR t2.previous_block_time IS NULL)
            AND 
            t1.block_time <= t2.block_time

        UNION ALL

        SELECT
            t2.address 
            ,t1.amount_raw AS amount_raw
            ,t2.event_sequence
        FROM    
            {{ ref('tokens_gnosis_base_without_suicide_transfers') }} t1
        INNER JOIN
            suicide_events t2
            ON 
            t2.address = t1."to"
        WHERE  
            t1.token_standard = 'native'
            AND 
            (t1.block_time > t2.previous_block_time OR t2.previous_block_time IS NULL)
            AND 
            t1.block_time <= t2.block_time

    )
    GROUP BY
        1, 2
),

suicide_balances AS (
    SELECT
        address
        ,event_sequence
        ,SUM(amount_raw) AS amount_raw
    FROM 
        tokens_gnosis_base_without_suicide_transfers
    GROUP BY 1, 2
)

SELECT 
    {{dbt_utils.generate_surrogate_key(['t2.block_number', 'tx.index', 'NULL', "array_join(t2.trace_address, ',')"])}} as unique_key
    , 'gnosis' as blockchain
    , t2.block_month
    , t2.block_date
    , t2.block_time
    , t2.block_number
    , t2.tx_hash
    , CAST(NULL AS INTEGER) AS evt_index
    , t2.trace_address
    , 'native' AS token_standard
    , t2.tx_from
    , t2.tx_to
    , t2.tx_index
    , t2.address AS "from"
    , t2.refund_address AS to
    , CAST(NULL AS varbinary) AS contract_address
    , CAST(t1.amount_raw AS UINT256) AS amount_raw
FROM 
    suicide_balances t1
INNER JOIN
    suicide_events t2
    ON 
    t2.address = t1.address
    AND 
    t2.event_sequence = t1.event_sequence
INNER JOIN {{ source('gnosis', 'transactions') }} tx ON
    cast(date_trunc('day', tx.block_time) as date) = t2.block_date 
    AND tx.block_number = t2.block_number
    AND tx.hash = t2.tx_hash
    {% if is_incremental() %}
    AND {{incremental_predicate('tx.block_time')}}
    {% endif %}
WHERE  
    CAST(t1.amount_raw AS UINT256) > UINT256 '0'