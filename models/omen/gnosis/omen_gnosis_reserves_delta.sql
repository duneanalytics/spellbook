{{ config(
    schema = 'omen_gnosis',
    alias = 'reserves_delta',
    
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
        ,shares
        ,amount_index 
        ,amount
        ,NULL AS fees
        ,action
        ,CASE
            WHEN action = 'Mint' THEN CAST(amounts AS INT256)
            WHEN action = 'Burn' THEN CAST(-amounts AS INT256)
        END AS reserves_delta
    FROM
        {{ ref('omen_gnosis_liquidity') }}
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
        ,outcomeTokens AS shares
        ,outcomeSlot AS amount_index
        ,amount
        ,feeAmount AS fees
        ,action
        ,CASE
            WHEN action = 'Buy' THEN CAST(amount AS INT256) - CAST(feeAmount + outcomeTokens AS INT256)
            WHEN action = 'Sell' THEN CAST(outcomeTokens AS INT256) - CAST(amount + feeAmount AS INT256)
        END AS reserves_delta
    FROM
        {{ ref('omen_gnosis_trades') }}
    {% if is_incremental() %}
    WHERE block_time >= date_trunc('day', now() - interval '7' day)
    {% else %}
    WHERE block_time >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
)

SELECT * FROM final
