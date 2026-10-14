{{ config(
    schema = 'addresses_events_arc'
    , alias = 'first_funded_by'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'append'
    , unique_key = ['address']
    , filtering_columns = []
    )
}}

-- Direct copy of addresses_events_first_funded_by. Arc EIP-7708 native
-- transfers have null trace addresses, so transfer ordering uses evt_index.
-- Keep trace_address in the standard output and preserve the macro's semantics.

SELECT 'arc' as blockchain
, tt.to AS address
, MIN_BY(
    tt."from"
    , (
        COALESCE(tt.block_number, -1)
        , COALESCE(tt.tx_index, -1)
        , COALESCE(tt.evt_index, -1)
    )
) AS first_funded_by
, MIN_BY(tt.tx_from, (COALESCE(tt.block_number, -1), COALESCE(tt.tx_index, -1))) AS first_funding_executed_by
, MIN_BY(
    tt.amount
    , (
        COALESCE(tt.block_number, -1)
        , COALESCE(tt.tx_index, -1)
        , COALESCE(tt.evt_index, -1)
    )
) AS amount
, MIN_BY(
    tt.amount_usd
    , (
        COALESCE(tt.block_number, -1)
        , COALESCE(tt.tx_index, -1)
        , COALESCE(tt.evt_index, -1)
    )
) AS amount_usd
, MIN(tt.block_time) AS block_time
, MIN(tt.block_number) AS block_number
, MIN_BY(tt.tx_hash, (COALESCE(tt.block_number, -1), COALESCE(tt.tx_index, -1))) AS tx_hash
, MIN_BY(tt.tx_index, (COALESCE(tt.block_number, -1), COALESCE(tt.tx_index, -1))) AS tx_index
, MIN_BY(
    tt.trace_address
    , (
        COALESCE(tt.block_number, -1)
        , COALESCE(tt.tx_index, -1)
        , COALESCE(tt.evt_index, -1)
    )
) AS trace_address
, MIN_BY(
    tt.unique_key
    , (
        COALESCE(tt.block_number, -1)
        , COALESCE(tt.tx_index, -1)
        , COALESCE(tt.evt_index, -1)
    )
) AS unique_key
FROM {{ source('tokens_arc', 'transfers') }} tt
{% if is_incremental() %}
WHERE {{ incremental_predicate('tt.block_time') }}
AND tt.token_standard = 'native'
AND tt.to NOT IN (SELECT address FROM {{this}})
{% else %}
WHERE tt.token_standard = 'native'
{% endif %}
GROUP BY tt.to

