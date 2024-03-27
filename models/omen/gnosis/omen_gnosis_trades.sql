{{ config(
    schema = 'omen_gnosis',
    alias = 'trades',
    
    partition_by = ['block_day'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_day', 'tx_hash', 'evt_index'],
    post_hook='{{ expose_spells(\'["gnosis"]\',
                                "project",
                                "omen",
                                \'["hdser"]\') }}'
    )
}}

{% set project_start_date = '2024-01-01' %}


WITH

trades AS (
    SELECT
        block_time
        ,DATE_TRUNC('day', block_time) AS block_day
        ,tx_from 
        ,tx_to 
        ,tx_hash 
        ,index AS evt_index
        ,contract_address AS evt_contract_address
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
        AND block_time >= date_trunc('day', now() - interval '7' day)
        {% else %}
        AND block_time >= TIMESTAMP '{{project_start_date}}'
        {% endif %}

),

prices AS (
    SELECT 
        DATE_TRUNC('day',minute) AS day
        ,MAX(contract_address) AS contract_address
        ,MAX(decimals) AS decimals
        ,MAX(symbol) AS symbol
        ,APPROX_PERCENTILE(price,0.5) AS price
    FROM
        {{ source('prices', 'usd') }}
    WHERE
        blockchain = 'gnosis'
        {% if is_incremental() %}
        AND {{ incremental_predicate('minute') }}
        {% else %}
        AND minute >= DATE '{{project_start_date}}'
        {% endif %}
    GROUP BY 1
),

final AS (
    SELECT
        t1.block_time
        ,t1.block_day
        ,t1.tx_from 
        ,t1.tx_to 
        ,t1.tx_hash 
        ,t1.evt_index
        ,t2.fixedProductMarketMaker
        ,t2.collateralToken
        ,t3.symbol
        ,t1.address
        ,t1.outcomeIndex
        ,t1.amount AS amount_raw
        ,t3.decimals
        ,t1.amount/POWER(10,t3.decimals) AS amount
        ,t1.amount/POWER(10,t3.decimals) * t3.price AS amount_usd
        ,t1.feeAmount/POWER(10,t3.decimals) AS feeAmount
        ,t1.feeAmount/POWER(10,t3.decimals) * t3.price AS feeAmount_usd
        ,t1.outcomeTokens
        ,t1.action
    FROM
        trades t1 
    LEFT JOIN 
        {{ ref('omen_gnosis_markets') }} t2
        ON
        t2.fixedProductMarketMaker = t1.evt_contract_address
    LEFT JOIN
        prices t3
        ON
        t3.contract_address = t2.collateralToken
        AND 
        t3.day = t1.block_day--date_trunc('minute', t1.block_time)
)

SELECT * FROM final
