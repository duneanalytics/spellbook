{% set blockchain = 'base' %}

{{
    config(
        schema = 'tapio_' + blockchain,
        alias = 'liquidity',
        materialized = 'view',
        file_format = 'delta'
    )
}}

{{ 
    tapio_compatible_liquidity_macro(
        blockchain = blockchain,
        project = 'tapio',
        version = '1',
        factory_create_pool_function = 'selfpeggingassetfactory_call_createpool',
        factory_create_pool_evt = 'selfpeggingassetfactory_evt_poolcreated',
        spa_minted_evt = 'selfpeggingasset_evt_minted',
        spa_redeemed_evt = 'selfpeggingasset_evt_redeemed',
        spa_swapped_evt = 'selfpeggingasset_evt_tokenswapped',
        spa_donated_evt = 'selfpeggingasset_evt_donated'

    )
}}