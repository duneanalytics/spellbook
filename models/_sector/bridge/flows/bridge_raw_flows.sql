{{ config(
    schema = 'bridge',
    alias = 'raw_flows',
    materialized = 'view',
    unique_key = ['blockchain','tx_hash','evt_index']
)
}}

{% set bridges_models = [
 ref('bridge_ethereum_flows')
] %}

with base_union as (
SELECT * FROM  (
{% for bridges_model in bridges_models %}
    SELECT blockchain
    , project
    , project_version
    , flows_type
    , block_month
    , block_time
    , block_number
    , amount_raw
    , "from"
    , to
    , local_token
    , remote_token
    , extra_data
    , tx_hash
    , evt_index
    , contract_address
    FROM {{ bridges_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
    )
)
select * from base_union
