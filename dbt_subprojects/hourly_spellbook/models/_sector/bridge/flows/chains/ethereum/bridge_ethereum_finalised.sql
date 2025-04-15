{% set blockchain = 'ethereum' %}

{{ config(
    schema = 'bridge_' + blockchain,
    alias = 'finalised',
    materialized = 'view'
    )
}}

{% set bridge_platforms = [
    'bridge_ethereum_base_bridge_finalised'
] %}

SELECT *
FROM (
    {% for bridge_platform in bridge_platforms %}
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
    , extra_data
    , tx_hash
    , evt_index
    , contract_address
    , bridge_id
    FROM {{ ref(bridge_platform) }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)