{% set blockchain = 'ethereum' %}

{{ config(
    schema = 'bridge_' + blockchain,
    alias = 'flows',
    materialized = 'view'
    )
}}

{% set bridge_platforms = [
    'bridge_ethereum_base_raw_flows'
] %}

SELECT *
FROM (
    {% for chain in chains %}
    SELECT blockchain
    , project
    , project_version
    , flows_type
    , block_month
    , block_time
    , block_number
    , amount_raw
    , sender
    , recipient
    , local_token
    , remote_token
    , extra_data
    , tx_hash
    , evt_index
    , contract_address
    FROM {{ ref(bridge_platforms) }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)