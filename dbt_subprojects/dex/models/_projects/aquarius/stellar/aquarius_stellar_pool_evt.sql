{{ config(
    schema = 'aquarius_stellar'
    , alias = 'pool_evt'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['block_date', 'surrogate_key']
    , partition_by = ['block_date']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

-- Router pool-registration events decoded from the raw Aquarius event index.
-- stellar.history_contract_events stores each Soroban event twice: once with
-- operation_id and once with a null operation_id. Keep only operation_id IS NOT NULL.

WITH events AS (
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
    WHERE e.event_name IN (
        'add_pool'
        , 'init_concentrated_pool'
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
    , json_extract_scalar(TRY(json_parse(data_decoded)), '$.vec[0].address') AS pool_address
    , json_extract_scalar(TRY(json_parse(data_decoded)), '$.vec[1].symbol') AS pool_type
    , json_extract_scalar(TRY(json_parse(topics_decoded)), '$[1].vec[0].address') AS token0
    , json_extract_scalar(TRY(json_parse(topics_decoded)), '$[1].vec[1].address') AS token1
    , json_extract_scalar(TRY(json_parse(topics_decoded)), '$[1].vec[2].address') AS token2
    , updated_at
    , ingested_at
FROM events
