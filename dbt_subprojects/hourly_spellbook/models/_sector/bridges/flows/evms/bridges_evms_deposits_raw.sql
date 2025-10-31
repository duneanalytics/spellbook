{{ config(
    schema = 'bridges_evms'
    , alias = 'deposits_raw'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy='merge'
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
)
}}

{% set chains = [
    'arbitrum'
    , 'avalanche_c'
    , 'base'
    , 'blast'
    , 'bnb'
    , 'ethereum'
    , 'hyperevm'
    , 'ink'
    , 'lens'
    , 'linea'
    , 'optimism'
    , 'plasma'
    , 'polygon'
    , 'scroll'
    , 'unichain'
    , 'worldchain'
    , 'zksync'
    , 'zora'
] %}

SELECT *
    FROM (
        {% for chain in chains %}
        SELECT d.deposit_chain
        , d.withdrawal_chain_id
        , d.withdrawal_chain
        , d.bridge_name
        , d.bridge_version
        , d.block_date
        , d.block_time
        , d.block_number
        , d.deposit_amount_raw
        , d.sender
        , d.recipient
        , d.deposit_token_standard
        , d.deposit_token_address
        , d.tx_from
        , d.tx_hash
        , d.evt_index
        , d.contract_address
        , d.bridge_transfer_id
        FROM {{ ref('bridges_'~chain~'_deposits') }} d
        {% if is_incremental() %}
        LEFT JOIN {{this}} t ON d.deposit_chain = '{{chain}}'
            AND d.bridge_name = t.bridge_name
            AND d.bridge_version = t.bridge_version
            AND d.withdrawal_chain_id = t.withdrawal_chain_id
            AND d.tx_hash = t.tx_hash
            AND d.evt_index = t.evt_index
        WHERE {{ incremental_predicate('d.block_time') }}
        AND t.bridge_transfer_id IS NULL
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %} 
        )
