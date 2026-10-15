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
-- Official router functions use u128 amounts. Observed event bodies:
-- swap: pool, token_in, token_out, in_amount, out_amount
-- deposit: pool, [amount0, amount1, ...], shares
-- withdraw: pool, shares, [amount0, amount1, ...]
-- claim: pool, reward_token, amount
-- config_rewards: pool, tps, unix_ts
-- Token order on topics is the sorted pool pair. Twin rows dropped via
-- operation_id IS NOT NULL.

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
        , TRY(json_parse(e.topics_decoded)) AS topics_json
        , TRY(json_parse(e.data_decoded)) AS data_json
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
    , json_extract_scalar(data_json, '$.vec[0].address') AS pool_address
    , json_extract_scalar(topics_json, '$[1].vec[0].address') AS token0
    , json_extract_scalar(topics_json, '$[1].vec[1].address') AS token1
    , json_extract_scalar(topics_json, '$[1].vec[2].address') AS token2
    , json_extract_scalar(topics_json, '$[2].address') AS trader
    , json_extract_scalar(data_json, '$.vec[1].address') AS token_in
    , json_extract_scalar(data_json, '$.vec[2].address') AS token_out
    , TRY_CAST(json_extract_scalar(data_json, '$.vec[3].u128') AS uint256) AS amount_in_raw
    , TRY_CAST(json_extract_scalar(data_json, '$.vec[4].u128') AS uint256) AS amount_out_raw
    , TRY_CAST(
        COALESCE(
            json_extract_scalar(data_json, '$.vec[1].vec[0].u128')
            , json_extract_scalar(data_json, '$.vec[2].vec[0].u128')
        ) AS uint256
    ) AS amount0_raw
    , TRY_CAST(
        COALESCE(
            json_extract_scalar(data_json, '$.vec[1].vec[1].u128')
            , json_extract_scalar(data_json, '$.vec[2].vec[1].u128')
        ) AS uint256
    ) AS amount1_raw
    , TRY_CAST(
        COALESCE(
            json_extract_scalar(data_json, '$.vec[1].vec[2].u128')
            , json_extract_scalar(data_json, '$.vec[2].vec[2].u128')
        ) AS uint256
    ) AS amount2_raw
    , TRY_CAST(
        CASE event_name
            WHEN 'deposit' THEN json_extract_scalar(data_json, '$.vec[2].u128')
            WHEN 'withdraw' THEN json_extract_scalar(data_json, '$.vec[1].u128')
        END AS uint256
    ) AS shares_raw
    , CASE
        WHEN event_name = 'claim' THEN json_extract_scalar(data_json, '$.vec[1].address')
    END AS reward_token
    , TRY_CAST(
        CASE event_name
            WHEN 'claim' THEN json_extract_scalar(data_json, '$.vec[2].u128')
            WHEN 'config_rewards' THEN json_extract_scalar(data_json, '$.vec[1].u128')
        END AS uint256
    ) AS reward_amount_raw
    , TRY_CAST(
        CASE
            WHEN event_name = 'config_rewards' THEN json_extract_scalar(data_json, '$.vec[2].u64')
        END AS bigint
    ) AS reward_config_time
    , updated_at
    , ingested_at
FROM events
