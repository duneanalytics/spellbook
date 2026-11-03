{{ config(
    schema = 'aquarius_stellar'
    , alias = 'liquidity_evt'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['block_date', 'surrogate_key']
    , partition_by = ['block_date']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

-- Pool liquidity and position events decoded from the raw Aquarius event index.
-- Official concentrated-pool event table:
-- deposit_liquidity / withdraw_liquidity: topics token0, token1; data liquidity, amount0, amount1
-- position_update: topics user; data tick_lower, tick_upper, liquidity_delta
-- claim_fees: topics owner, token0, token1; data amount0, amount1
-- claim_reward: topics reward_token, user; data amount
-- Classic constant / stable pools use the same deposit/withdraw body:
-- shares first, then token amounts. Twin rows dropped via operation_id IS NOT NULL.

WITH pools AS (
    SELECT
        pool
        , pool_type
        , token0
        , token1
        , token2
    FROM {{ ref('aquarius_stellar_pools') }}
)

, events AS (
    SELECT
        e.surrogate_key
        , e.blockchain
        , e.project
        , e.version
        , e.block_date
        , e.block_time
        , e.ledger_sequence
        , e.tx_hash
        , e.transaction_id
        , e.operation_id
        , e.contract_id
        , e.successful
        , e.in_successful_contract_call
        , e.type
        , e.type_string
        , e.event_name
        , e.topics
        , e.topics_decoded
        , e.data
        , e.data_decoded
        , e.contract_event_xdr
        , p.pool
        , p.pool_type
        , p.token0
        , p.token1
        , p.token2
        , TRY(json_parse(e.topics_decoded)) AS topics_json
        , TRY(json_parse(e.data_decoded)) AS data_json
        , e.updated_at
        , e.ingested_at
    FROM {{ ref('aquarius_stellar_evt') }} e
    INNER JOIN pools p
        ON p.pool = e.contract_id
    WHERE e.event_name IN (
        'deposit_liquidity'
        , 'withdraw_liquidity'
        , 'position_update'
        , 'claim_fees'
        , 'claim_reward'
    )
        AND e.operation_id IS NOT NULL
        {% if is_incremental() -%}
        AND {{ incremental_predicate('e.block_time') }}
        {% endif -%}
)

SELECT
    surrogate_key
    , blockchain
    , project
    , version
    , block_date
    , block_time
    , ledger_sequence
    , tx_hash
    , transaction_id
    , operation_id
    , contract_id
    , successful
    , in_successful_contract_call
    , type
    , type_string
    , event_name
    , topics
    , topics_decoded
    , data
    , data_decoded
    , contract_event_xdr
    , pool
    , pool_type
    , token0
    , token1
    , token2
    , CASE event_name
        WHEN 'deposit_liquidity' THEN json_extract_scalar(topics_json, '$[1].address')
        WHEN 'withdraw_liquidity' THEN json_extract_scalar(topics_json, '$[1].address')
        WHEN 'claim_fees' THEN json_extract_scalar(topics_json, '$[2].address')
        WHEN 'claim_reward' THEN json_extract_scalar(topics_json, '$[1].address')
    END AS asset0
    , CASE event_name
        WHEN 'deposit_liquidity' THEN json_extract_scalar(topics_json, '$[2].address')
        WHEN 'withdraw_liquidity' THEN json_extract_scalar(topics_json, '$[2].address')
        WHEN 'claim_fees' THEN json_extract_scalar(topics_json, '$[3].address')
    END AS asset1
    , CASE
        WHEN event_name IN ('deposit_liquidity', 'withdraw_liquidity')
            THEN json_extract_scalar(topics_json, '$[3].address')
    END AS asset2
    , CASE event_name
        WHEN 'position_update' THEN json_extract_scalar(topics_json, '$[1].address')
        WHEN 'claim_fees' THEN json_extract_scalar(topics_json, '$[1].address')
        WHEN 'claim_reward' THEN json_extract_scalar(topics_json, '$[2].address')
    END AS user_address
    , TRY_CAST(
        CASE
            WHEN event_name IN ('deposit_liquidity', 'withdraw_liquidity')
                THEN COALESCE(
                    json_extract_scalar(data_json, '$.vec[0].i128')
                    , json_extract_scalar(data_json, '$.vec[0].u128')
                )
        END AS int256
    ) AS shares_raw
    , TRY_CAST(
        CASE event_name
            WHEN 'deposit_liquidity' THEN COALESCE(
                json_extract_scalar(data_json, '$.vec[1].i128')
                , json_extract_scalar(data_json, '$.vec[1].u128')
            )
            WHEN 'withdraw_liquidity' THEN COALESCE(
                json_extract_scalar(data_json, '$.vec[1].i128')
                , json_extract_scalar(data_json, '$.vec[1].u128')
            )
            WHEN 'claim_fees' THEN COALESCE(
                json_extract_scalar(data_json, '$.vec[0].i128')
                , json_extract_scalar(data_json, '$.vec[0].u128')
            )
            WHEN 'claim_reward' THEN COALESCE(
                json_extract_scalar(data_json, '$.vec[0].i128')
                , json_extract_scalar(data_json, '$.vec[0].u128')
            )
        END AS int256
    ) AS amount0_raw
    , TRY_CAST(
        CASE event_name
            WHEN 'deposit_liquidity' THEN COALESCE(
                json_extract_scalar(data_json, '$.vec[2].i128')
                , json_extract_scalar(data_json, '$.vec[2].u128')
            )
            WHEN 'withdraw_liquidity' THEN COALESCE(
                json_extract_scalar(data_json, '$.vec[2].i128')
                , json_extract_scalar(data_json, '$.vec[2].u128')
            )
            WHEN 'claim_fees' THEN COALESCE(
                json_extract_scalar(data_json, '$.vec[1].i128')
                , json_extract_scalar(data_json, '$.vec[1].u128')
            )
        END AS int256
    ) AS amount1_raw
    , TRY_CAST(
        CASE
            WHEN event_name IN ('deposit_liquidity', 'withdraw_liquidity')
                THEN COALESCE(
                    json_extract_scalar(data_json, '$.vec[3].i128')
                    , json_extract_scalar(data_json, '$.vec[3].u128')
                )
        END AS int256
    ) AS amount2_raw
    , TRY_CAST(
        CASE
            WHEN event_name = 'position_update' THEN json_extract_scalar(data_json, '$.vec[0].i32')
        END AS integer
    ) AS tick_lower
    , TRY_CAST(
        CASE
            WHEN event_name = 'position_update' THEN json_extract_scalar(data_json, '$.vec[1].i32')
        END AS integer
    ) AS tick_upper
    , TRY_CAST(
        CASE
            WHEN event_name = 'position_update' THEN COALESCE(
                json_extract_scalar(data_json, '$.vec[2].i128')
                , json_extract_scalar(data_json, '$.vec[2].u128')
            )
        END AS int256
    ) AS liquidity_delta
    , updated_at
    , ingested_at
FROM events
