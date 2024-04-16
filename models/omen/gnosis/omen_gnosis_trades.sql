{{ config(
    schema = 'omen_gnosis',
    alias = 'trades',
    
    partition_by = ['block_day'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_day', 'tx_hash', 'evt_index','outcomeSlot'],
    post_hook='{{ expose_spells(\'["gnosis"]\',
                                "project",
                                "omen",
                                \'["hdser"]\') }}'
    )
}}

{% set project_start_date = '2020-12-01' %}


WITH


trades AS (
    SELECT
        block_time
        ,DATE_TRUNC('day', block_time) AS block_day
        ,tx_from 
        ,tx_to 
        ,tx_hash 
        ,index AS evt_index
        ,contract_address AS fixedProductMarketMaker
        ,varbinary_substring(topic1,13,20)  AS address
        ,varbinary_to_uint256(varbinary_ltrim(topic2)) AS outcomeIndex
        ,varbinary_to_uint256(varbinary_ltrim(varbinary_substring(data,1,32)))AS amount
        ,varbinary_to_uint256(varbinary_ltrim(varbinary_substring(data,33,32))) AS feeAmount
        ,varbinary_to_uint256(varbinary_ltrim(varbinary_substring(data,65,32))) AS outcomeTokens
        ,CASE 
            WHEN topic0 = 0x4f62630f51608fc8a7603a9391a5101e58bd7c276139366fc107dc3b67c3dcf8 
                THEN 'Buy'
            ELSE 'Sell'
        END AS action
    FROM
        {{ source('gnosis', 'logs') }}
    WHERE
        (
            topic0 = 0x4f62630f51608fc8a7603a9391a5101e58bd7c276139366fc107dc3b67c3dcf8 -- Buy
            OR
            topic0 = 0xadcf2a240ed9300d681d9a3f5382b6c1beed1b7e46643e0c7b42cbe6e2d766b4 -- Sell
        )
        {% if is_incremental() %}
        AND 
        block_time >= date_trunc('day', now() - interval '7' day)
        AND 
        {{ incremental_predicate('block_time') }}
        {% else %}
        AND block_time >= TIMESTAMP '{{project_start_date}}'
        {% endif %}

),

trades_slot AS (
    SELECT 
        t1.block_time
        ,t1.block_day
        ,t1.tx_from 
        ,t1.tx_to 
        ,t1.tx_hash 
        ,t1.evt_index
        ,t1.fixedProductMarketMaker
        ,t1.address
        ,t1.outcomeIndex
        ,SEQUENCE(0,COALESCE(CARDINALITY(t2.partition),CARDINALITY(t3.partition)) - 1 ) AS outcomeSlot
        ,t1.amount
        ,t1.feeAmount
        ,TRANSFORM(SEQUENCE(0, COALESCE(CARDINALITY(t2.partition),CARDINALITY(t3.partition)) - 1 ), x -> IF(x = t1.outcomeIndex, t1.outcomeTokens, 0)) AS outcomeTokens_amount
        ,t1.action
    FROM
        trades  t1
    LEFT JOIN 
        {{source('omen_gnosis','ConditionalTokens_evt_PositionsMerge') }} t2
        ON
        t2.evt_tx_hash = t1.tx_hash
        AND
        t2.evt_index < t1.evt_index
        AND
        t2.amount = CAST(t1.amount AS UINT256) + CAST(t1.feeAmount AS UINT256)
    LEFT JOIN 
        {{source('omen_gnosis','ConditionalTokens_evt_PositionSplit') }} t3
        ON
        t3.evt_tx_hash = t1.tx_hash
        AND
        t3.evt_index < t1.evt_index
        AND
        t3.amount = CAST(t1.amount AS UINT256) - CAST(t1.feeAmount AS UINT256)
),

final AS (
    SELECT 
        *
        ,TRANSFORM(
            outcomeTokens_amount, 
            x -> CASE
                    WHEN action = 'Buy' THEN CAST(amount AS INT256) - CAST(feeAmount + x AS INT256)
                    WHEN action = 'Sell' THEN CAST(x AS INT256) - CAST(amount + feeAmount AS INT256)
                END
        ) AS reserves_delta
    FROM    
        trades_slot
)

SELECT * FROM final