{{ config(
    schema = 'sushiswap_v3_stellar'
    , alias = 'evt'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['block_date', 'surrogate_key']
    , partition_by = ['block_date']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set project_start_date = '2026-03-01' %}

-- Raw index of SushiSwap V3 Stellar contract events.
-- Follows factory contracts plus pool addresses seen on factory pool_created
-- events. The only decode is event_name. Does not depend on the pools table.

WITH factory AS (
    SELECT contract_id
    FROM {{ ref('sushiswap_v3_stellar_factories') }}
    WHERE contract_name = 'factory'
)

, created_pools AS (
    SELECT DISTINCT
        json_extract_scalar(
            element_at(
                filter(
                    CAST(COALESCE(json_extract(TRY(json_parse(e.data_decoded)), '$.map'), JSON '[]') AS array(json))
                    , x -> json_extract_scalar(x, '$.key.symbol') = 'pool_address'
                )
                , 1
            )
            , '$.val.address'
        ) AS pool_address
    FROM {{ source('stellar', 'history_contract_events') }} e
    INNER JOIN factory f
        ON f.contract_id = e.contract_id
    WHERE e.successful
        AND e.type_string = 'ContractEventTypeContract'
        AND json_extract_scalar(TRY(json_parse(e.topics_decoded)), '$[0].symbol') = 'pool_created'
        {% if is_incremental() -%}
        AND {{ incremental_predicate('e.closed_at') }}
        {% else -%}
        AND e.closed_at_date >= DATE '{{ project_start_date }}'
        {% endif -%}
)

, contracts AS (
    SELECT contract_id
    FROM {{ ref('sushiswap_v3_stellar_factories') }}
    UNION
    SELECT pool_address AS contract_id
    FROM created_pools
    WHERE pool_address IS NOT NULL
    {% if is_incremental() -%}
    UNION
    SELECT DISTINCT contract_id
    FROM {{ this }}
    {% endif -%}
)

, events AS (
    SELECT
        e.closed_at_date AS block_date
        , e.closed_at AS block_time
        , e.ledger_sequence
        , e.transaction_hash AS tx_hash
        , e.transaction_id
        , e.operation_id
        , e.contract_id
        , e.successful
        , e.in_successful_contract_call
        , e.type
        , e.type_string
        , json_extract_scalar(TRY(json_parse(e.topics_decoded)), '$[0].symbol') AS event_name
        , e.topics
        , e.topics_decoded
        , e.data
        , e.data_decoded
        , e.contract_event_xdr
        , e.updated_at
        , e.ingested_at
    FROM {{ source('stellar', 'history_contract_events') }} e
    INNER JOIN contracts c
        ON c.contract_id = e.contract_id
    WHERE e.successful
        AND e.type_string = 'ContractEventTypeContract'
        {% if is_incremental() -%}
        AND {{ incremental_predicate('e.closed_at') }}
        {% else -%}
        AND e.closed_at_date >= DATE '{{ project_start_date }}'
        {% endif -%}
)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'block_date'
        , 'to_hex(tx_hash)'
        , 'transaction_id'
        , 'operation_id'
        , 'contract_id'
        , 'event_name'
        , 'topics'
        , 'data'
    ]) }} AS surrogate_key
    , CAST('stellar' AS varchar) AS blockchain
    , CAST('sushiswap' AS varchar) AS project
    , CAST('3' AS varchar) AS version
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
    , updated_at
    , ingested_at
FROM events
