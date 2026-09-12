{{ config(
    schema = 'sushiswap_v3_stellar'
    , alias = 'pool_evt'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['block_date', 'surrogate_key']
    , partition_by = ['block_date']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

-- Factory and pool lifecycle events decoded from the raw SushiSwap V3 Stellar event index.
-- stellar.history_contract_events stores each Soroban event twice: once with
-- operation_id and once with a null operation_id. The null twin has the same
-- payload, so keep only operation_id IS NOT NULL.

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
        , CAST(COALESCE(json_extract(TRY(json_parse(e.data_decoded)), '$.map'), JSON '[]') AS array(json)) AS data_map
        , e.updated_at
        , e.ingested_at
    FROM {{ ref('sushiswap_v3_stellar_evt') }} e
    WHERE e.event_name IN (
        'pool_created'
        , 'pool_upgraded'
        , 'pool_migrated'
        , 'wasm_approved'
        , 'set_protocol_fee'
        , 'init'
        , 'upgraded'
        , 'migrated'
    )
        AND e.operation_id IS NOT NULL
        {% if is_incremental() -%}
        AND {{ incremental_predicate('e.block_time') }}
        {% endif -%}
)

, decoded AS (
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
        , json_extract_scalar(
            element_at(
                filter(data_map, x -> json_extract_scalar(x, '$.key.symbol') = 'pool_address')
                , 1
            )
            , '$.val.address'
        ) AS pool_address
        , json_extract_scalar(
            element_at(
                filter(data_map, x -> json_extract_scalar(x, '$.key.symbol') = 'token0')
                , 1
            )
            , '$.val.address'
        ) AS token0
        , json_extract_scalar(
            element_at(
                filter(data_map, x -> json_extract_scalar(x, '$.key.symbol') = 'token1')
                , 1
            )
            , '$.val.address'
        ) AS token1
        , TRY_CAST(json_extract_scalar(
            element_at(
                filter(data_map, x -> json_extract_scalar(x, '$.key.symbol') = 'fee')
                , 1
            )
            , '$.val.u32'
        ) AS integer) AS fee
        , TRY_CAST(json_extract_scalar(
            element_at(
                filter(data_map, x -> json_extract_scalar(x, '$.key.symbol') = 'tick_spacing')
                , 1
            )
            , '$.val.i32'
        ) AS integer) AS tick_spacing
        , json_extract_scalar(
            element_at(
                filter(data_map, x -> json_extract_scalar(x, '$.key.symbol') = 'sender')
                , 1
            )
            , '$.val.address'
        ) AS sender
        , TRY_CAST(json_extract_scalar(
            element_at(
                filter(data_map, x -> json_extract_scalar(x, '$.key.symbol') = 'sqrt_price_x96')
                , 1
            )
            , '$.val.u256'
        ) AS uint256) AS sqrt_price_x96
        , TRY_CAST(json_extract_scalar(
            element_at(
                filter(data_map, x -> json_extract_scalar(x, '$.key.symbol') = 'tick')
                , 1
            )
            , '$.val.i32'
        ) AS integer) AS tick
        , updated_at
        , ingested_at
    FROM events
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
    , pool_address
    , token0
    , token1
    , fee
    , tick_spacing
    , sender
    , sqrt_price_x96
    , tick
    , updated_at
    , ingested_at
FROM decoded
