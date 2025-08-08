{{ config(
    schema = 'bridges_evms'
    , alias = 'deposits'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy='merge'
    , unique_key = ['deposit_chain','withdrawal_chain','bridge_name','bridge_version','bridge_transfer_id']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
)
}}

{% set chains = [
    'ethereum'
    , 'base'
    , 'arbitrum'
    , 'avalanche_c'
    , 'optimism'
    , 'polygon'
    , 'unichain'
] %}

WITH grouped_deposits AS (
    SELECT *
    FROM (
        {% for chain in chains %}
        SELECT deposit_chain
        , withdrawal_chain
        , bridge_name
        , bridge_version
        , block_date
        , block_time
        , block_number
        , deposit_amount_raw
        , sender
        , recipient
        , deposit_token_standard
        , deposit_token_address
        , tx_from
        , tx_hash
        , evt_index
        , contract_address
        , bridge_transfer_id
        FROM {{ ref('bridges_'~chain~'_deposits') }}
        {% if is_incremental() %}
        WHERE  {{ incremental_predicate('block_time') }}
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %} 
        )
    )

SELECT d.deposit_chain
, d.withdrawal_chain
, d.bridge_name
, d.bridge_version
, d.block_date
, d.block_time
, d.block_number
, d.deposit_amount_raw
, d.deposit_amount_raw/POWER(10, p.decimals) AS deposit_amount
, p.price*d.deposit_amount_raw/POWER(10, p.decimals) AS deposit_amount_usd
, d.sender
, d.recipient
, d.deposit_token_standard
, d.deposit_token_address
, d.tx_from
, d.tx_hash
, d.evt_index
, d.contract_address
, d.bridge_transfer_id
FROM grouped_deposits d
INNER JOIN {{ source('prices', 'usd') }} p ON p.blockchain=d.deposit_chain
    AND p.contract_address=d.deposit_token_address
    AND p.minute=date_trunc('minute', d.block_time)
    {% if is_incremental() %}
    AND {{ incremental_predicate('p.minute') }}
    {% endif %}