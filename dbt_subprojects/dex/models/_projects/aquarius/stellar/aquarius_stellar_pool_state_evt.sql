{{ config(
    schema = 'aquarius_stellar'
    , alias = 'pool_state_evt'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['block_date', 'surrogate_key']
    , partition_by = ['block_date']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

-- Pool reserve / state snapshots decoded from the raw Aquarius event index.
-- Constant-product and stable pools emit update_reserves; concentrated also
-- emits pool_state. Twin source rows are dropped via operation_id IS NOT NULL.

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
        , e.updated_at
        , e.ingested_at
    FROM {{ ref('aquarius_stellar_evt') }} e
    INNER JOIN pools p
        ON p.pool = e.contract_id
    WHERE e.event_name IN (
        'update_reserves'
        , 'pool_state'
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
    , TRY_CAST(json_extract_scalar(TRY(json_parse(data_decoded)), '$.vec[0].i128') AS int256) AS reserve0_raw
    , TRY_CAST(json_extract_scalar(TRY(json_parse(data_decoded)), '$.vec[1].i128') AS int256) AS reserve1_raw
    , TRY_CAST(json_extract_scalar(TRY(json_parse(data_decoded)), '$.vec[2].i128') AS int256) AS reserve2_raw
    , updated_at
    , ingested_at
FROM events
