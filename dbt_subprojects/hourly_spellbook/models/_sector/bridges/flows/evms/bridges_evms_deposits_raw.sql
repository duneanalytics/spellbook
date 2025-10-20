{{ config(
    schema = 'bridges_evms'
    , alias = 'deposits_raw'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy='append'
    , unique_key = ['deposit_chain','withdrawal_chain','withdrawal_chain_id','bridge_name','bridge_version','bridge_transfer_id', 'tx_hash', 'evt_index']
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
        , withdrawal_chain_id
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
        FROM {{ ref('bridges_'~chain~'_deposits') }} d
        {% if is_incremental() %}
        LEFT JOIN {{this}} t ON d.deposit_chain = '{{chain}}'
            AND d.bridge_name = t.bridge_name
            AND d.bridge_version = t.bridge_version
            AND d.withdrawal_chain_id = t.withdrawal_chain_id
            AND d.tx_hash = t.tx_hash
            AND d.evt_index = t.evt_index
        WHERE {{ incremental_predicate('block_time') }}
        AND t.bridge_transfer_id IS NULL
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %} 
        )
