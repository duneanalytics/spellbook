{{ config(
    schema = 'omen_gnosis',
    alias = 'resolution',
    
    partition_by = ['block_day'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_time', 'tx_hash', 'evt_index', 'payoutNumerators_index'],
    post_hook='{{ expose_spells(\'["gnosis"]\',
                                "project",
                                "omen",
                                \'["hdser"]\') }}'
    )
}}

{% set project_start_date = '2020-12-01' %}


WITH 

ConditionResolution AS (
    SELECT
        t.block_time
        ,DATE_TRUNC('DAY', t.block_time) AS block_day
        ,t.tx_hash
        ,t.tx_from
        ,t.tx_to
        ,t.evt_index
        ,t.evt_contract_address
        ,t.conditionId
        ,t.oracle
        ,t.questionId
        --,t.outcomeSlotCount
        ,s.SEQUENCE_NUMBER -1 AS payoutNumerators_index
        ,VARBINARY_TO_UINT256(VARBINARY_LTRIM(VARBINARY_SUBSTRING(t.data, 65 + 32 * s.SEQUENCE_NUMBER, 32))) AS payoutNumerator
    FROM (
        SELECT
            block_time
            ,tx_hash
            ,tx_from
            ,tx_to
            ,index AS evt_index
            ,contract_address AS evt_contract_address
            ,topic1 AS conditionId
            ,VARBINARY_SUBSTRING(topic2,13,20) AS oracle
            ,topic3 AS questionId
            ,VARBINARY_TO_UINT256(VARBINARY_LTRIM(VARBINARY_SUBSTRING(data, 1, 32))) AS outcomeSlotCount
            ,VARBINARY_TO_UINT256(VARBINARY_LTRIM(VARBINARY_SUBSTRING(data, 65, 32))) AS payoutNumerators_size
            ,data
        FROM 
            {{source('gnosis','logs') }}
        WHERE
            --ConditionalTokens_evt_ConditionResolution
            topic0 = 0xb44d84d3289691f71497564b85d4233648d9dbae8cbdbb4329f301c3a0185894 
            {% if is_incremental() %}
            AND block_time >= date_trunc('day', now() - interval '7' day)
            {% else %}
            AND block_time >= TIMESTAMP '{{project_start_date}}'
            {% endif %}
    ) AS t
    CROSS JOIN UNNEST(SEQUENCE(1, TRY_CAST(t.payoutNumerators_size AS INTEGER)) ) AS s(SEQUENCE_NUMBER)
   
)

SELECt * FROM ConditionResolution