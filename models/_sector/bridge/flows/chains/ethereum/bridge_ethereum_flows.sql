{% set blockchain = 'ethereum' %}

{{ config(
    schema = 'bridge_' + blockchain,
    alias = 'flows',
    partition_by = ['blockchain','project','block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['blockchain','tx_hash','evt_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set bridges_models = [
 ref('bridge_ethereum_base_raw_flows')
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
    , sender
    , recipient
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
