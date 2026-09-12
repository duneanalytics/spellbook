{{ config(
    schema = 'aquarius_stellar'
    , alias = 'router_evt'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['block_date', 'surrogate_key']
    , partition_by = ['block_date']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

-- Router user-flow events decoded from the raw Aquarius event index.
-- Router emits swap / deposit / withdraw / claim in addition to pool-level
-- trade and deposit_liquidity / withdraw_liquidity.
-- Twin source rows are dropped via operation_id IS NOT NULL.

WITH factory AS (
    SELECT contract_id
    FROM {{ ref('aquarius_stellar_factories') }}
    WHERE contract_name = 'router'
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
        , e.updated_at
        , e.ingested_at
    FROM {{ ref('aquarius_stellar_evt') }} e
    INNER JOIN factory f
        ON f.contract_id = e.contract_id
    WHERE e.event_name IN (
        'swap'
        , 'deposit'
        , 'withdraw'
        , 'claim'
        , 'config_rewards'
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
    , COALESCE(
        json_extract_scalar(TRY(json_parse(data_decoded)), '$.vec[0].address')
        , json_extract_scalar(TRY(json_parse(topics_decoded)), '$[1].address')
    ) AS pool_address
    , COALESCE(
        json_extract_scalar(TRY(json_parse(data_decoded)), '$.vec[1].address')
        , json_extract_scalar(TRY(json_parse(topics_decoded)), '$[1].vec[0].address')
    ) AS token0
    , COALESCE(
        json_extract_scalar(TRY(json_parse(data_decoded)), '$.vec[2].address')
        , json_extract_scalar(TRY(json_parse(topics_decoded)), '$[1].vec[1].address')
    ) AS token1
    , json_extract_scalar(TRY(json_parse(topics_decoded)), '$[2].address') AS trader
    , TRY_CAST(json_extract_scalar(TRY(json_parse(data_decoded)), '$.vec[3].i128') AS int256) AS amount0_raw
    , TRY_CAST(json_extract_scalar(TRY(json_parse(data_decoded)), '$.vec[4].i128') AS int256) AS amount1_raw
    , TRY_CAST(json_extract_scalar(TRY(json_parse(data_decoded)), '$.vec[1].u128') AS uint256) AS reward_amount_raw
    , TRY_CAST(json_extract_scalar(TRY(json_parse(data_decoded)), '$.vec[2].u64') AS bigint) AS reward_config_time
    , updated_at
    , ingested_at
FROM events
