{{ config(
    schema = 'bridges_evms'
    , alias = 'deposits_raw'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy='merge'
    , unique_key = ['deposit_chain','withdrawal_chain','bridge_name','bridge_version','bridge_transfer_id', 'tx_hash', 'evt_index']
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
