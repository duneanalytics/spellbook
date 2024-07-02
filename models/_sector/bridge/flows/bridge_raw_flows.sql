{{ config(
    schema = 'bridge',
    alias = 'raw_flows',
    materialized = 'view',
    unique_key = ['blockchain','tx_hash','evt_index']
)
}}

{% set chains = [
     'ethereum'
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
    FROM {{ ref('bridge_'~chain~'_flows') }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)