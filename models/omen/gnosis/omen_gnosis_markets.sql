{{ config(
    schema = 'omen_gnosis',
    alias = 'markets',
    
    partition_by = ['block_day'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['fixedProductMarketMaker', 'questionId', 'conditionId', 'conditionIds_index'],
    post_hook='{{ expose_spells(\'["gnosis"]\',
                                "project",
                                "omen",
                                \'["hdser"]\') }}'
    )
}}

{% set project_start_date = '2020-12-01' %}

WITH 


Realitio_LogNewQuestion AS (
    SELECT
        contract_address,
        evt_tx_hash,
        evt_tx_from,
        evt_tx_to,
        evt_index,
        evt_block_time,
        evt_block_number,
        evt_block_date,
        arbitrator,
        content_hash,
        created,
        nonce,
        opening_ts,
        question,
        question_id,
        template_id,
        timeout,
        user
    FROM 
        {{source('omen_gnosis','Realitio_v2_1_evt_LogNewQuestion') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% else %}
    WHERE evt_block_time >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
), 

QuestionIdAnnouncement AS (
    -- RealitioScalarAdapter contract
    SELECT
        block_time AS evt_block_time,
        --block_hash,
        tx_hash AS evt_tx_hash,
        contract_address,
        topic1 AS realitioQuestionId,
        topic2 AS conditionQuestionId,
        VARBINARY_TO_UINT256(VARBINARY_LTRIM(VARBINARY_SUBSTRING(data, 1, 32))) AS low,
        VARBINARY_TO_UINT256(VARBINARY_LTRIM(VARBINARY_SUBSTRING(data, 33, 32))) AS high
    FROM 
        {{source('gnosis','logs') }}
    WHERE
        --QuestionIdAnnouncement
        topic0 = 0xab038c0885722fffdf6864cf016c56fa921a1506541dac4fcd59d65963916cb1
        {% if is_incremental() %}
        AND block_time >= date_trunc('day', now() - interval '7' day)
        {% else %}
        AND block_time >= TIMESTAMP '{{project_start_date}}'
        {% endif %}
        
),

ConditionPreparation AS (
    SELECT
        contract_address,
        evt_tx_hash,
        evt_tx_from,
        evt_tx_to,
        evt_index,
        evt_block_time,
        evt_block_number,
        evt_block_date,
        conditionId,
        oracle,
        outcomeSlotCount,
        questionId
    FROM 
        {{source('omen_gnosis','ConditionalTokens_evt_ConditionPreparation') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% else %}
    WHERE evt_block_time >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
), 

ConditionPreparation_end AS (
    SELECT 
        t1.evt_block_time,
        COALESCE(t2.evt_tx_hash, t1.evt_tx_hash) AS evt_tx_hash,
        t1.contract_address,
        t1.conditionId,
        t1.oracle,
        COALESCE(t2.realitioQuestionId, t1.questionId) AS questionId,
        t1.outcomeSlotCount
    FROM
        ConditionPreparation t1
    LEFT JOIN
        QuestionIdAnnouncement t2
        ON
        t2.evt_tx_hash = t1.evt_tx_hash
        AND
        t2.conditionQuestionId = t1.questionId
),

FixedProductMarketMakerCreation AS (
    SELECT
        contract_address,
        evt_tx_hash,
        evt_tx_from,
        evt_tx_to,
        evt_index,
        evt_block_time,
        evt_block_number,
        evt_block_date,
        collateralToken,
        conditionalTokens,
        creator,
        fee,
        fixedProductMarketMaker,
        conditionId
    FROM 
        {{source('omen_gnosis','FPMMDeterministicFactory_evt_FixedProductMarketMakerCreation') }}
        ,UNNEST(conditionIds) t(conditionId)
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% else %}
    WHERE evt_block_time >= TIMESTAMP '{{project_start_date}}'
    {% endif %} 
        
),



prediction_market_info AS (
    SELECT
        DATE_TRUNC('day', t1.evt_block_time) AS block_day,
        t1.evt_block_time AS block_time_LogNewQuestion,
        t1.evt_tx_hash AS tx_hash_LogNewQuestion,

        t2.evt_block_time AS block_time_ConditionPreparationn,
        t2.evt_tx_hash AS tx_hash_ConditionPreparation,

        t3.evt_block_time AS block_time_FixedProductMarketMakerCreation,
        t3.evt_tx_hash AS tx_hash_FixedProductMarketMakerCreation,

        t2.questionId,
        t2.conditionId,
        t2.oracle,
        t2.outcomeSlotCount,
        t1.question,
        t3.fixedProductMarketMaker,
        t3.conditionalTokens,
        t3.collateralToken,
        t3.fee,
        REGEXP_REPLACE(REVERSE(SPLIT_PART(REVERSE(t1.question), '␟', 2)), '[^A-Za-z]', '') AS category,
        FROM_UNIXTIME(t1.opening_ts) AS opening_time,
        t1.timeout,
        FROM_UNIXTIME(t1.created) AS creation_time
    FROM 
        Realitio_LogNewQuestion t1
    INNER JOIN 
        ConditionPreparation_end t2
        ON 
        t2.questionId = t1.question_id
    INNER JOIN 
        FixedProductMarketMakerCreation t3
        ON 
        t3.conditionId = t2.conditionId
)

SELECT
  *
FROM prediction_market_info