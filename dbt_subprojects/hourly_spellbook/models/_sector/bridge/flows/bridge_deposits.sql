{{ config(
    schema = 'bridge'
    , alias = 'initiated'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy='merge'
    , unique_key = ['deposit_chain','tx_hash','evt_index']
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
        SELECT deposit_chain
        , withdraw_chain
        , project
        , project_version
        , event_side
        , block_date
        , block_time
        , block_number
        , deposit_amount_raw
        , withdraw_amount_raw
        , sender
        , recipient
        , deposit_token_standard
        , withdraw_token_standard
        , deposit_token_address
        , withdraw_token_address
        , tx_from
        , tx_hash
        , evt_index
        , contract_address
        , bridge_id
        FROM {{ ref('bridge_'~chain~'_deposits') }}
        {% if is_incremental() %}
        WHERE  {{ incremental_predicate('block_time') }}
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %} 
        )
    )

, deposit_filled AS (
    SELECT deposit_chain
    , withdraw_chain
    , project
    , project_version
    , event_side
    , block_date
    , block_time
    , block_number
    , sender
    , recipient
    , deposit_amount_raw
    , deposit_amount_raw/POWER(10, pus.decimals) AS deposit_amount
    , pus.price*deposit_amount_raw/POWER(10, pus.decimals) AS deposit_amount_usd
    , deposit_token_address
    , deposit_token_standard
    , pus.symbol AS deposit_token_symbol
    , withdraw_amount_raw
    , withdraw_token_address
    , withdraw_token_standard
    , tx_from
    , tx_hash
    , evt_index
    , i.contract_address
    , bridge_id
    FROM grouped_initiated_events i
    INNER JOIN {{ source('prices', 'usd') }} pus ON pus.blockchain=i.deposit_chain
        AND pus.contract_address=i.deposit_token_address
        AND pus.minute=date_trunc('minute', block_time)
        {% if is_incremental() %}
        AND  {{ incremental_predicate('pus.minute') }}
        {% endif %}
    )

SELECT deposit_chain
, withdraw_chain
, project
, project_version
, event_side
, block_date
, block_time
, block_number
, sender
, recipient
, deposit_amount_raw
, deposit_amount
, deposit_amount_usd
, deposit_token_address
, deposit_token_standard
, deposit_token_symbol
, withdraw_amount_raw
, withdraw_amount_raw/POWER(10, pud.decimals) AS withdraw_amount
, pud.price*withdraw_amount_raw/POWER(10, pud.decimals) AS withdraw_amount_usd
, withdraw_token_address
, withdraw_token_standard
, pud.symbol AS withdraw_token_symbol
, tx_from
, tx_hash
, evt_index
, i.contract_address
, bridge_id
FROM deposit_filled i
INNER JOIN {{ source('prices', 'usd') }} pud ON pud.blockchain=i.withdraw_chain
    AND pud.contract_address=i.withdraw_token_address
    AND pud.minute=date_trunc('minute', block_time)
        {% if is_incremental() %}
        AND  {{ incremental_predicate('pud.minute') }}
        {% endif %}