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
        block_time,
        block_hash,
        tx_hash,
        contract_address,
        topic1 AS questionId,
        VARBINARY_SUBSTRING(topic2, 13, 20) AS user,
        topic3 AS content_hash,
        VARBINARY_TO_UINT256(VARBINARY_LTRIM(VARBINARY_SUBSTRING(data, 1, 32))) AS template_id,
        FROM_UTF8(VARBINARY_SUBSTRING(data, 225)) AS question,
        VARBINARY_SUBSTRING(VARBINARY_SUBSTRING(data, 65, 32), 13, 20) AS arbitrator,
        VARBINARY_TO_UINT256(VARBINARY_LTRIM(VARBINARY_SUBSTRING(data, 97, 32))) AS timeout,
        VARBINARY_TO_UINT256(VARBINARY_LTRIM(VARBINARY_SUBSTRING(data, 129, 32))) AS opening_ts,
        VARBINARY_TO_UINT256(VARBINARY_LTRIM(VARBINARY_SUBSTRING(data, 161, 32))) AS nonce,
        VARBINARY_TO_UINT256(VARBINARY_LTRIM(VARBINARY_SUBSTRING(data, 193, 32))) AS created
    FROM 
        {{source('gnosis','logs') }}
    WHERE
        topic0 = 0xfe2dac156a3890636ce13f65f4fdf41dcaee11526e4a5374531572d92194796c --Realitio_v2_1_evt_LogNewQuestion
        {% if is_incremental() %}
        AND block_time >= date_trunc('day', now() - interval '7' day)
        {% else %}
        AND block_time >= TIMESTAMP '{{project_start_date}}'
        {% endif %}
), 

QuestionIdAnnouncement AS (
    SELECT
        block_time,
        block_hash,
        tx_hash,
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
        block_time,
        block_hash,
        tx_hash,
        contract_address,
        topic1 AS conditionId,
        VARBINARY_SUBSTRING(topic2, 13, 20) AS oracle,
        topic3 AS questionId,
        VARBINARY_TO_UINT256(VARBINARY_LTRIM(VARBINARY_SUBSTRING(data, 1, 32))) AS outcomeSlotCount
    FROM 
        {{source('gnosis','logs') }}
    WHERE
        topic0 = 0xab3760c3bd2bb38b5bcf54dc79802ed67338b4cf29f3054ded67ed24661e4177 --ConditionalTokens_evt_ConditionPreparation
        {% if is_incremental() %}
        AND block_time >= date_trunc('day', now() - interval '7' day)
        {% else %}
        AND block_time >= TIMESTAMP '{{project_start_date}}'
        {% endif %}
), 

ConditionPreparation_end AS (
    SELECT 
        t1.block_time,
        t1.block_hash,
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
        t2.tx_hash = t1.tx_hash
        AND
        t2.conditionQuestionId = t1.questionId
),

FixedProductMarketMakerCreation AS (
    SELECT
        t.block_time,
        t.block_hash,
        t.contract_address,
        t.creator,
        t.fixedProductMarketMaker,
        t.conditionalTokens,
        t.collateralToken,
        t.fee,
        s.SEQUENCE_NUMBER AS conditionIds_index,
        VARBINARY_SUBSTRING(t.data, 161 + 32 * s.SEQUENCE_NUMBER, 32) AS conditionId
    FROM (
        SELECT
            block_time,
            block_hash,
            contract_address,
            VARBINARY_SUBSTRING(topic1, 13, 20)  AS creator,
            VARBINARY_SUBSTRING(VARBINARY_SUBSTRING(data, 1, 32), 13, 20) AS fixedProductMarketMaker,
            VARBINARY_SUBSTRING(VARBINARY_SUBSTRING(data, 33, 32), 13, 20) AS conditionalTokens,
            VARBINARY_SUBSTRING(VARBINARY_SUBSTRING(data, 65, 32), 13, 20) AS collateralToken,
            VARBINARY_TO_UINT256(VARBINARY_LTRIM(VARBINARY_SUBSTRING(data, 129, 32))) AS fee,
            VARBINARY_TO_UINT256(VARBINARY_SUBSTRING(data, 161, 32)) AS conditionIds_size,
            data
        FROM 
            {{source('gnosis','logs') }}
        WHERE
            topic0 = 0x92e0912d3d7f3192cad5c7ae3b47fb97f9c465c1dd12a5c24fd901ddb3905f43 --FPMMDeterministicFactory_evt_FixedProductMarketMakerCreation
            {% if is_incremental() %}
            AND block_time >= date_trunc('day', now() - interval '7' day)
            {% else %}
            AND block_time >= TIMESTAMP '{{project_start_date}}'
            {% endif %}
        ) AS t
    CROSS JOIN UNNEST(SEQUENCE(1, TRY_CAST(t.conditionIds_size AS INTEGER)) ) AS s(SEQUENCE_NUMBER)
),



prediction_market_info AS (
  SELECT
    t1.block_time,
    DATE_TRUNC('day', t1.block_time) AS block_day,
    t1.tx_hash,
    t2.questionId,
    t2.conditionId,
    t2.oracle,
    t2.outcomeSlotCount,
    t1.question,
    t3.fixedProductMarketMaker,
    t3.conditionalTokens,
    t3.collateralToken,
    t3.fee,
    t3.conditionIds_index,
    REGEXP_REPLACE(REVERSE(SPLIT_PART(REVERSE(t1.question), '␟', 2)), '[^A-Za-z]', '') AS category,
    FROM_UNIXTIME(t1.opening_ts) AS opening_time,
    FROM_UNIXTIME(t1.opening_ts+t1.timeout) AS closing_time,
    FROM_UNIXTIME(t1.created) AS creation_time
  FROM Realitio_LogNewQuestion AS t1
  INNER JOIN ConditionPreparation_end AS t2
    ON t2.questionId = t1.questionId
  INNER JOIN FixedProductMarketMakerCreation AS t3
    ON 
    t3.conditionId = t2.conditionId
)

SELECT
  *
FROM prediction_market_info