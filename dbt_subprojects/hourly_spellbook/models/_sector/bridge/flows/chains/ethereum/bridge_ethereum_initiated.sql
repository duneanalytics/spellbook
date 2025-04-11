{% set blockchain = 'ethereum' %}

{{ config(
    schema = 'bridge_' + blockchain,
    alias = 'initiated',
    materialized = 'view'
    )
}}

{% set bridge_platforms = [
    'bridge_ethereum_base_bridge_initiated'
] %}

SELECT *
FROM (
    {% for bridge_platform in bridge_platforms %}
    SELECT source_blockchain
    , destination_blockchain
    , project
    , project_version
    , event_side
    , block_month
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
    FROM {{ ref(bridge_platform) }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)