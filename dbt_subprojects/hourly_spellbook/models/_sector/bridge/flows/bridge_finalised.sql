{{ config(
    schema = 'bridge',
    alias = 'finalised',
    materialized = 'view',
    unique_key = ['source_blockchain','tx_hash','evt_index']
)
}}

{% set chains = [
    'ethereum'
    , 'base'
] %}

SELECT *
FROM (
    {% for chain in chains %}
    SELECT source_blockchain
    , destination_blockchain
    , project
    , project_version
    , event_side
    , block_date
    , block_time
    , block_number
    , amount_raw
    , source_address
    , destination_address
    , source_token_standard
    , destination_token_standard
    , source_token_address
    , destination_token_address
    , extra_data
    , tx_hash
    , evt_index
    , contract_address
    FROM {{ ref('bridge_'~chain~'_finalised') }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)