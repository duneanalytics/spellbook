{{ config(
        schema = 'angstrom',
        alias = 'pools',
        post_hook='{{ expose_spells(blockchains = \'["ethereum"]\',
                                      spell_type = "project", 
                                      spell_name = "angstrom", 
                                      contributors = \'["jnoorchashm37", "Henrystats"]\') }}'
        )
}}

{% set angstrom_models = [
ref('angstrom_ethereum_pools')
] %}


select *
from (
    {% for dex_pool_model in angstrom_models %}
    select
        blockchain
        , pool_id
        , block_number
        , bundle_fee 
        , unlocked_fee 
        , protocol_unlocked_fee 
        , token0
        , token1
    from 
    {{ dex_pool_model }}
    {% if not loop.last %}
    union all
    {% endif %}
    {% endfor %}
)