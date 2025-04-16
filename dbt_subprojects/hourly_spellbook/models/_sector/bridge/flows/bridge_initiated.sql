{{ config(
    schema = 'bridge'
    , alias = 'initiated'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy='merge'
    , unique_key = ['source_chain','tx_hash','evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
)
}}

{% set chains = [
    'ethereum'
    , 'base'
] %}

WITH grouped_initiated_events AS (
    SELECT *
    FROM (
        {% for chain in chains %}
        SELECT source_chain
        , destination_chain
        , project
        , project_version
        , event_side
        , block_date
        , block_time
        , block_number
        , source_amount_raw
        , destination_amount_raw
        , source_address
        , destination_address
        , source_token_standard
        , destination_token_standard
        , source_token_address
        , destination_token_address
    , tx_from
        , tx_hash
        , evt_index
        , contract_address
        , bridge_id
        FROM {{ ref('bridge_'~chain~'_initiated') }}
        {% if is_incremental() %}
        WHERE  {{ incremental_predicate('block_time') }}
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %} 
        )
    )

, source_filled AS (
    SELECT source_chain
    , destination_chain
    , project
    , project_version
    , event_side
    , block_date
    , block_time
    , block_number
    , source_address
    , destination_address
    , source_amount_raw
    , source_amount_raw/POWER(10, pus.decimals) AS source_amount
    , pus.price*source_amount_raw/POWER(10, pus.decimals) AS source_amount_usd
    , source_token_address
    , source_token_standard
    , pus.symbol AS source_token_symbol
    , destination_amount_raw
    , destination_token_address
    , destination_token_standard
    , tx_hash
    , evt_index
    , i.contract_address
    FROM grouped_initiated_events i
    INNER JOIN {{ source('prices', 'usd') }} pus ON pus.blockchain=i.source_chain
        AND pus.contract_address=i.source_token_address
        AND pus.minute=date_trunc('minute', block_time)
        {% if is_incremental() %}
        AND  {{ incremental_predicate('pus.minute') }}
        {% endif %}
    )

SELECT source_chain
, destination_chain
, project
, project_version
, event_side
, block_date
, block_time
, block_number
, source_address
, destination_address
, source_amount_raw
, source_amount
, source_amount_usd
, source_token_address
, source_token_standard
, source_token_symbol
, destination_amount_raw
, destination_amount_raw/POWER(10, pud.decimals) AS destination_amount
, pud.price*destination_amount_raw/POWER(10, pud.decimals) AS destination_amount_usd
, destination_token_address
, destination_token_standard
, pud.symbol AS destination_token_symbol
, tx_hash
, evt_index
, i.contract_address
FROM source_filled i
INNER JOIN {{ source('prices', 'usd') }} pud ON pud.blockchain=i.destination_chain
    AND pud.contract_address=i.destination_token_address
    AND pud.minute=date_trunc('minute', block_time)
        {% if is_incremental() %}
        AND  {{ incremental_predicate('pud.minute') }}
        {% endif %}