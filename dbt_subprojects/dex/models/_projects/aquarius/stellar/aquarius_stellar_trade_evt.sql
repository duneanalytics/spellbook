{{ config(
    schema = 'aquarius_stellar'
    , alias = 'trade_evt'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['block_date', 'surrogate_key']
    , partition_by = ['block_date']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

-- Pool trade events decoded from the raw Aquarius event index.
-- Pool contracts emit trade; the router swap event is decoded separately.
-- Twin source rows are dropped via operation_id IS NOT NULL.

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
    WHERE e.event_name = 'trade'
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
    , json_extract_scalar(TRY(json_parse(topics_decoded)), '$[1].address') AS token_sold_address
    , json_extract_scalar(TRY(json_parse(topics_decoded)), '$[2].address') AS token_bought_address
    , json_extract_scalar(TRY(json_parse(topics_decoded)), '$[3].address') AS trader
    , TRY_CAST(json_extract_scalar(TRY(json_parse(data_decoded)), '$.vec[0].i128') AS int256) AS token_sold_amount_raw
    , TRY_CAST(json_extract_scalar(TRY(json_parse(data_decoded)), '$.vec[1].i128') AS int256) AS token_bought_amount_raw
    , TRY_CAST(json_extract_scalar(TRY(json_parse(data_decoded)), '$.vec[2].i128') AS int256) AS fee_raw
    , updated_at
    , ingested_at
FROM events
