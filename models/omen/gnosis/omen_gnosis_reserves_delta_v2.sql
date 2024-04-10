{{ config(
    schema = 'omen_gnosis',
    alias = 'reserves_delta2',
    
    partition_by = ['block_day'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_day', 'tx_hash', 'evt_index','amount_index'],
    post_hook='{{ expose_spells(\'["gnosis"]\',
                                "project",
                                "omen",
                                \'["hdser"]\') }}'
    )
}}

{% set project_start_date = '2020-12-01' %}

WITH final AS (
    SELECT 
        block_time
        ,block_day
        ,tx_from 
        ,tx_to 
        ,tx_hash
        ,evt_index
        ,fixedProductMarketMaker
        ,funder AS address
       -- ,shares
        ,outcomeIndex 
        ,outcomeTokens_amount
        --,NULL AS amount
        ,NULL AS fees
        ,action
        ,TRANSFORM(
            outcomeTokens_amount, 
            x -> CASE
                    WHEN action = 'Add' THEN CAST(x AS INT256)
                    WHEN action = 'Remove' THEN CAST(-x AS INT256)
                END
        ) AS reserves_delta
    FROM
        {{ ref('omen_gnosis_liquidity_v2') }}
    {% if is_incremental() %}
    WHERE block_time >= date_trunc('day', now() - interval '7' day)
    {% else %}
    WHERE block_time >= TIMESTAMP '{{project_start_date}}'
    {% endif %}

    UNION ALL

    SELECT 
         block_time
        ,block_day
        ,tx_from 
        ,tx_to 
        ,tx_hash
        ,evt_index
        ,fixedProductMarketMaker
        ,address
        --,NULL AS shares
        ,outcomeSlot AS outcomeIndex
        ,outcomeTokens_amount
        --,amount
        ,feeAmount AS fees
        ,action
        ,TRANSFORM(
            outcomeTokens_amount, 
            x -> CASE
                    WHEN action = 'Buy' THEN CAST(amount AS INT256) - CAST(feeAmount + x AS INT256)
                    WHEN action = 'Sell' THEN CAST(x AS INT256) - CAST(amount + feeAmount AS INT256)
                END
        ) AS reserves_delta
    FROM
        {{ ref('omen_gnosis_trades_v2') }}
    {% if is_incremental() %}
    WHERE block_time >= date_trunc('day', now() - interval '7' day)
    {% else %}
    WHERE block_time >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
)

SELECT * FROM final
