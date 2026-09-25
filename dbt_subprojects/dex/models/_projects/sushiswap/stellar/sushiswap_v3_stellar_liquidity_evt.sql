{{ config(
    schema = 'sushiswap_v3_stellar'
    , alias = 'liquidity_evt'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['block_date', 'surrogate_key']
    , partition_by = ['block_date']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

-- Mint, burn, and collect events decoded from the raw SushiSwap V3 Stellar event index.
-- stellar.history_contract_events stores each Soroban event twice: once with
-- operation_id and once with a null operation_id. The null twin has the same
-- payload, so keep only operation_id IS NOT NULL.

WITH pools AS (
    SELECT
        pool
        , token0
        , token1
        , fee
        , tick_spacing
    FROM {{ ref('sushiswap_v3_stellar_pools') }}
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
        , p.token0
        , p.token1
        , p.fee
        , p.tick_spacing
        , CAST(COALESCE(json_extract(TRY(json_parse(e.data_decoded)), '$.map'), JSON '[]') AS array(json)) AS data_map
        , e.updated_at
        , e.ingested_at
    FROM {{ ref('sushiswap_v3_stellar_evt') }} e
    INNER JOIN pools p
        ON p.pool = e.contract_id
    WHERE e.event_name IN (
        'mint'
        , 'burn'
        , 'collect'
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
    , token0
    , token1
    , fee
    , tick_spacing
    , json_extract_scalar(
        element_at(
            filter(data_map, x -> json_extract_scalar(x, '$.key.symbol') = 'sender')
            , 1
        )
        , '$.val.address'
    ) AS sender
    , json_extract_scalar(
        element_at(
            filter(data_map, x -> json_extract_scalar(x, '$.key.symbol') = 'owner')
            , 1
        )
        , '$.val.address'
    ) AS owner
    , json_extract_scalar(
        element_at(
            filter(data_map, x -> json_extract_scalar(x, '$.key.symbol') = 'recipient')
            , 1
        )
        , '$.val.address'
    ) AS recipient
    , TRY_CAST(json_extract_scalar(
        element_at(
            filter(data_map, x -> json_extract_scalar(x, '$.key.symbol') = 'amount')
            , 1
        )
        , '$.val.u128'
    ) AS uint256) AS amount
    , COALESCE(
        TRY_CAST(json_extract_scalar(
            element_at(
                filter(data_map, x -> json_extract_scalar(x, '$.key.symbol') = 'amount0')
                , 1
            )
            , '$.val.i128'
        ) AS int256)
        , TRY_CAST(json_extract_scalar(
            element_at(
                filter(data_map, x -> json_extract_scalar(x, '$.key.symbol') = 'amount0')
                , 1
            )
            , '$.val.u128'
        ) AS int256)
    ) AS amount0
    , COALESCE(
        TRY_CAST(json_extract_scalar(
            element_at(
                filter(data_map, x -> json_extract_scalar(x, '$.key.symbol') = 'amount1')
                , 1
            )
            , '$.val.i128'
        ) AS int256)
        , TRY_CAST(json_extract_scalar(
            element_at(
                filter(data_map, x -> json_extract_scalar(x, '$.key.symbol') = 'amount1')
                , 1
            )
            , '$.val.u128'
        ) AS int256)
    ) AS amount1
    , TRY_CAST(json_extract_scalar(
        element_at(
            filter(data_map, x -> json_extract_scalar(x, '$.key.symbol') = 'tick_lower')
            , 1
        )
        , '$.val.i32'
    ) AS integer) AS tick_lower
    , TRY_CAST(json_extract_scalar(
        element_at(
            filter(data_map, x -> json_extract_scalar(x, '$.key.symbol') = 'tick_upper')
            , 1
        )
        , '$.val.i32'
    ) AS integer) AS tick_upper
    , updated_at
    , ingested_at
FROM events
