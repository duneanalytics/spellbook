{{ config(
    schema = 'omen_gnosis',
    alias = 'liquidity',
    
    partition_by = ['block_day'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_time', 'tx_hash', 'evt_index'],
    post_hook='{{ expose_spells(blockchains  = \'["gnosis"]\',
                                spell_type   = "project",
                                spell_name   = "omen",
                                contributors = \'["hdser"]\') }}'
    )
}}

{% set project_start_date = '2020-12-01' %}

WITH

FPMMFundingAdded AS (
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
        SEQUENCE(0, TRY_CAST(VARBINARY_TO_UINT256(VARBINARY_LTRIM(VARBINARY_SUBSTRING(data, 65, 32))) AS INTEGER) - 1) AS outcomeIndex,
        TRANSFORM(
            SEQUENCE(1, TRY_CAST(VARBINARY_TO_UINT256(VARBINARY_LTRIM(VARBINARY_SUBSTRING(data, 65, 32))) AS INTEGER) )
            , x -> VARBINARY_TO_UINT256(
                VARBINARY_LTRIM(
                    VARBINARY_SUBSTRING(data, 65 + 32 * x, 32)
                )
            )
        ) AS outcomeTokens_amount
    FROM 
        {{source('gnosis','logs') }}
    WHERE
        --FPMMDeterministicFactory_evt_FPMMFundingAdded
        topic0 = 0xec2dc3e5a3bb9aa0a1deb905d2bd23640d07f107e6ceb484024501aad964a951 
        {% if is_incremental() %}
        AND {{ incremental_predicate('block_time') }}
        {% else %}
        AND block_time >= TIMESTAMP '{{project_start_date}}'
        {% endif %}
   
),

add_liquidity AS (
    SELECT 
        block_time,
        DATE_TRUNC('day', block_time) AS block_day,
        tx_from,
        tx_to,
        tx_hash,
        evt_index,
        evt_contract_address AS fixedProductMarketMaker,
        funder,
        sharesMinted AS shares,
        NULL AS collateralRemovedFromFeePool,
        outcomeIndex,
        outcomeTokens_amount,
        'Add' AS action,
        TRANSFORM(outcomeTokens_amount, x -> CAST(x AS INT256)) AS reserves_delta
    FROM
        FPMMFundingAdded
),


FPMMFundingRemoved AS (
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
        SEQUENCE(0, TRY_CAST(VARBINARY_TO_UINT256(VARBINARY_LTRIM(VARBINARY_SUBSTRING(data, 97, 32))) AS INTEGER) - 1) AS outcomeIndex,
        TRANSFORM(
            SEQUENCE(1, TRY_CAST(VARBINARY_TO_UINT256(VARBINARY_LTRIM(VARBINARY_SUBSTRING(data, 97, 32))) AS INTEGER) )
            , x -> VARBINARY_TO_UINT256(
                VARBINARY_LTRIM(
                    VARBINARY_SUBSTRING(data, 97 + 32 * x, 32)
                )
            )
        ) AS outcomeTokens_amount
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
),

remove_liquidity AS (
    SELECT 
        block_time,
        DATE_TRUNC('day', block_time) AS block_day,
        tx_from,
        tx_to,
        tx_hash,
        evt_index,
        evt_contract_address AS fixedProductMarketMaker,
        funder,
        sharesBurnt AS shares,
        collateralRemovedFromFeePool,
        outcomeIndex,
        outcomeTokens_amount,
        'Remove' AS action,
        TRANSFORM(outcomeTokens_amount, x -> CAST(-x AS INT256)) AS reserves_delta
    FROM
        FPMMFundingRemoved 
),

final AS (
    SELECT * FROM add_liquidity
    UNION ALL
    SELECT * FROM remove_liquidity
)

SELECt * FROM final