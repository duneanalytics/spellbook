{{ config(
    schema = 'omen_gnosis',
    alias = 'trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_day', 'tx_hash', 'evt_index','outcomeslot'],
    post_hook='{{ expose_spells(blockchains  = \'["gnosis"]\',
                                spell_type   = "project",
                                spell_name   = "omen",
                                contributors = \'["hdser"]\') }}'
    )
}}

{% set project_start_date = '2020-12-01' %}

WITH

trades AS (
    SELECT
        block_time
        ,CAST(block_time AS DATE) AS block_day
        ,tx_from 
        ,tx_to 
        ,tx_hash 
        ,index AS evt_index
        ,contract_address AS fixedproductmarketmaker
        ,varbinary_substring(topic1,13,20)  AS address
        ,varbinary_to_uint256(varbinary_ltrim(topic2)) AS outcomeindex
        ,varbinary_to_uint256(varbinary_ltrim(varbinary_substring(data,1,32)))AS amount
        ,varbinary_to_uint256(varbinary_ltrim(varbinary_substring(data,33,32))) AS feeamount
        ,varbinary_to_uint256(varbinary_ltrim(varbinary_substring(data,65,32))) AS outcometokens
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
        ,t1.fixedproductmarketmaker
        ,t1.address
        ,t1.outcomeindex
        ,SEQUENCE(0,COALESCE(CARDINALITY(t2.partition),CARDINALITY(t3.partition)) - 1 ) AS outcomeslot
        ,t1.amount
        ,t1.feeamount
        ,TRANSFORM(SEQUENCE(0, COALESCE(CARDINALITY(t2.partition),CARDINALITY(t3.partition)) - 1 ), x -> IF(x = t1.outcomeindex, t1.outcometokens, 0)) AS outcometokens_amount
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
        t2.amount = CAST(t1.amount AS UINT256) + CAST(t1.feeamount AS UINT256)
    LEFT JOIN 
        {{source('omen_gnosis','ConditionalTokens_evt_PositionSplit') }} t3
        ON
        t3.evt_tx_hash = t1.tx_hash
        AND
        t3.evt_index < t1.evt_index
        AND
        t3.amount = CAST(t1.amount AS UINT256) - CAST(t1.feeamount AS UINT256)
),

final AS (
    SELECT 
        *
        ,TRANSFORM(
            outcometokens_amount, 
            x -> CASE
                    WHEN action = 'Buy' THEN CAST(amount AS INT256) - CAST(feeamount + x AS INT256)
                    WHEN action = 'Sell' THEN CAST(x AS INT256) - CAST(amount + feeamount AS INT256)
                END
        ) AS reserves_delta
    FROM    
        trades_slot
)

SELECT  
    block_time
    ,block_day
    ,tx_from 
    ,tx_to 
    ,tx_hash 
    ,evt_index
    ,fixedproductmarketmaker
    ,address
    ,outcomeindex
    ,outcomeslot
    ,amount
    ,feeamount
    ,outcometokens_amount
    ,action
    ,reserves_delta
FROM final