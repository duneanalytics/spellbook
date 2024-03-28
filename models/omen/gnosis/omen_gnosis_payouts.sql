{{ config(
    schema = 'omen_gnosis',
    alias = 'payouts',
    
    partition_by = ['block_day'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_time', 'tx_hash', 'evt_index', 'indexSets_index'],
    post_hook='{{ expose_spells(\'["gnosis"]\',
                                "project",
                                "omen",
                                \'["hdser"]\') }}'
    )
}}

{% set project_start_date = '2020-12-01' %}


WITH 

PayoutRedemption AS (
    SELECT
        t.block_time,
        DATE_TRUNC('DAY', t.block_time) AS block_day,
        t.tx_hash,
        t.evt_index,
        t.tx_from,
        t.tx_to,
        t.evt_contract_address,
        t.redeemer,
        t.collateralToken,
        t.parentCollectionId,
        t.conditionId,
        t.payout,
        s.SEQUENCE_NUMBER -1 AS indexSets_index,
        VARBINARY_TO_UINT256(VARBINARY_LTRIM(VARBINARY_SUBSTRING(t.data, 97 + 32 * s.SEQUENCE_NUMBER, 32))) AS indexSets
    FROM (
        SELECT
            block_time
            ,tx_hash
            ,tx_from
            ,tx_to
            ,index AS evt_index
            ,contract_address AS evt_contract_address
            ,VARBINARY_SUBSTRING(topic1,13,20) AS redeemer
            ,VARBINARY_SUBSTRING(topic2,13,20) AS collateralToken
            ,topic3 AS parentCollectionId
            ,VARBINARY_SUBSTRING(data, 1, 32) AS conditionId
            ,VARBINARY_TO_UINT256(VARBINARY_LTRIM(VARBINARY_SUBSTRING(data, 65, 32))) AS payout
            ,VARBINARY_TO_UINT256(VARBINARY_LTRIM(VARBINARY_SUBSTRING(data, 97, 32))) AS indexSets_size
            ,data
        FROM 
            {{source('gnosis','logs') }}
        WHERE
            --ConditionalTokens_evt_PayoutRedemption
            topic0 = 0x2682012a4a4f1973119f1c9b90745d1bd91fa2bab387344f044cb3586864d18d 
            {% if is_incremental() %}
            AND block_time >= date_trunc('day', now() - interval '7' day)
            {% else %}
            AND block_time >= TIMESTAMP '{{project_start_date}}'
            {% endif %}
    ) AS t
    CROSS JOIN UNNEST(SEQUENCE(1, TRY_CAST(t.indexSets_size AS INTEGER)) ) AS s(SEQUENCE_NUMBER)
   
)

SELECt * FROM PayoutRedemption