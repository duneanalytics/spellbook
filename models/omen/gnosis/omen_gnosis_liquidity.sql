{{ config(
    schema = 'omen_gnosis',
    alias = 'liquidity',
    
    partition_by = ['block_day'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_time', 'tx_hash', 'evt_index'],
    post_hook='{{ expose_spells(\'["gnosis"]\',
                                "project",
                                "omen",
                                \'["hdser"]\') }}'
    )
}}

{% set project_start_date = '2020-12-01' %}


WITH 

FPMMFundingAdded AS (
    SELECT
        t.block_time,
        t.tx_from,
        t.tx_to,
        t.tx_hash,
        t.evt_index,
        t.evt_contract_address,
        t.funder,
        t.sharesMinted,
        ARRAY_AGG(s.SEQUENCE_NUMBER - 1 ORDER BY s.SEQUENCE_NUMBER) AS outcomeIndex,
        ARRAY_AGG(
            VARBINARY_TO_UINT256(
                VARBINARY_LTRIM(
                    VARBINARY_SUBSTRING(t.data, 65 + 32 * s.SEQUENCE_NUMBER, 32)
                )
            ) ORDER BY s.SEQUENCE_NUMBER) AS outcomeTokens_amount
    FROM (
        SELECT
            block_time,
            tx_hash,
            tx_from,
            tx_to,
            index AS evt_index,
            contract_address AS evt_contract_address,
            VARBINARY_SUBSTRING(topic1, 13, 20) AS funder,
            VARBINARY_TO_UINT256(VARBINARY_LTRIM(VARBINARY_SUBSTRING(data, 33, 32))) AS sharesMinted,
            VARBINARY_TO_UINT256(VARBINARY_LTRIM(VARBINARY_SUBSTRING(data, 65, 32))) AS amountsAdded_size,
            data
        FROM 
            {{source('gnosis','logs') }}
        WHERE
            --FPMMDeterministicFactory_evt_FPMMFundingAdded
            topic0 = 0xec2dc3e5a3bb9aa0a1deb905d2bd23640d07f107e6ceb484024501aad964a951 
            {% if is_incremental() %}
            AND block_time >= date_trunc('day', now() - interval '7' day)
            {% else %}
            AND block_time >= TIMESTAMP '{{project_start_date}}'
            {% endif %}
    ) AS t
    CROSS JOIN UNNEST(SEQUENCE(1, TRY_CAST(t.amountsAdded_size AS INTEGER)) ) AS s(SEQUENCE_NUMBER)
    GROUP BY
        1,2,3,4,5,6,7,8
   
),

ConditionalTokens_evt_PositionSplit AS (
    SELECT
        contract_address,
        evt_tx_hash,
        evt_tx_from,
        evt_tx_to,
        evt_index,
        evt_block_time,
        evt_block_number,
        evt_block_date,
        amount,
        collateralToken,
        conditionId,
        parentCollectionId,
        partition,
        stakeholder
    FROM 
        {{source('omen_gnosis','ConditionalTokens_evt_PositionSplit') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% else %}
    WHERE evt_block_time >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
), 

add_liquidity AS (
    SELECT 
        t1.block_time,
        DATE_TRUNC('day', t1.block_time) AS block_day,
        t1.tx_from,
        t1.tx_to,
        t1.tx_hash,
        t1.evt_index,
        t1.evt_contract_address AS fixedProductMarketMaker,
        t1.funder,
        t1.sharesMinted AS shares,
        NULL AS collateralRemovedFromFeePool,
        t1.outcomeIndex,
        t1.outcomeTokens_amount,
        COALESCE(t2.amount,0) AS amount,
        'Add' AS action
    FROM
        FPMMFundingAdded t1
    LEFT JOIN
        ConditionalTokens_evt_PositionSplit t2 
    ON 
        t2.evt_tx_hash = t1.tx_hash
),

FPMMFundingRemoved AS (
    SELECT
        t.block_time,
        t.tx_from,
        t.tx_to,
        t.tx_hash,
        t.evt_index,
        t.evt_contract_address,
        t.funder,
        t.collateralRemovedFromFeePool,
        t.sharesBurnt,
        ARRAY_AGG(s.SEQUENCE_NUMBER - 1 ORDER BY s.SEQUENCE_NUMBER) AS outcomeIndex,
        ARRAY_AGG(
            VARBINARY_TO_UINT256(
                VARBINARY_LTRIM(
                    VARBINARY_SUBSTRING(t.data, 97 + 32 * s.SEQUENCE_NUMBER, 32)
                )
            ) ORDER BY s.SEQUENCE_NUMBER) AS outcomeTokens_amount
    FROM (
        SELECT
            block_time,
            tx_hash,
            tx_from,
            tx_to,
            index AS evt_index,
            contract_address AS evt_contract_address,
            VARBINARY_SUBSTRING(topic1, 13, 20) AS funder,
            VARBINARY_TO_UINT256(VARBINARY_LTRIM(VARBINARY_SUBSTRING(data, 33, 32))) AS collateralRemovedFromFeePool,
            VARBINARY_TO_UINT256(VARBINARY_LTRIM(VARBINARY_SUBSTRING(data, 65, 32))) AS sharesBurnt,
            VARBINARY_TO_UINT256(VARBINARY_LTRIM(VARBINARY_SUBSTRING(data, 97, 32))) AS amountsRemoved_size,
            data
        FROM 
            {{source('gnosis','logs') }}
        WHERE
            --FPMMDeterministicFactory_evt_FPMMFundingRemoved
            topic0 = 0x8b4b2c8ebd04c47fc8bce136a85df9b93fcb1f47c8aa296457d4391519d190e7
            {% if is_incremental() %}
            AND 
            block_time >= date_trunc('day', now() - interval '7' day)
            AND 
            {{ incremental_predicate('block_time') }}
            {% else %}
            AND 
            block_time >= TIMESTAMP '{{project_start_date}}'
            {% endif %}
    ) AS t
    CROSS JOIN UNNEST(SEQUENCE(1, TRY_CAST(t.amountsRemoved_size AS INTEGER)) ) AS s(SEQUENCE_NUMBER) 
    GROUP BY
        1,2,3,4,5,6,7,8,9
),


ConditionalTokens_evt_PositionsMerge AS (
    SELECT
        contract_address,
        evt_tx_hash,
        evt_tx_from,
        evt_tx_to,
        evt_index,
        evt_block_time,
        evt_block_number,
        evt_block_date,
        amount,
        collateralToken,
        conditionId,
        parentCollectionId,
        partition,
        stakeholder
    FROM 
        {{source('omen_gnosis','ConditionalTokens_evt_PositionsMerge') }}
    {% if is_incremental() %}
    WHERE 
        evt_block_time >= date_trunc('day', now() - interval '7' day)
        AND 
        {{ incremental_predicate('evt_block_time') }}
    {% else %}
    WHERE evt_block_time >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
), 

remove_liquidity AS (
    SELECT 
        t1.block_time,
        DATE_TRUNC('day', t1.block_time) AS block_day,
        t1.tx_from,
        t1.tx_to,
        t1.tx_hash,
        t1.evt_index,
        t1.evt_contract_address AS fixedProductMarketMaker,
        t1.funder,
        t1.sharesBurnt AS shares,
        t1.collateralRemovedFromFeePool,
        t1.outcomeIndex,
        t1.outcomeTokens_amount,
        COALESCE(t2.amount,0) AS amount,
        'Remove' AS action
    FROM
        FPMMFundingRemoved t1
    LEFT JOIN
        ConditionalTokens_evt_PositionsMerge t2 
    ON 
        t2.evt_tx_hash = t1.tx_hash
),

final AS (
    SELECT * FROM add_liquidity
    UNION ALL
    SELECT * FROM remove_liquidity
)

SELECt * FROM final