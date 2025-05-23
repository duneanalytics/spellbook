{% set blockchain = 'sonic' %}

{{
    config(
        schema = 'tapio_sonic',
        alias = 'liquidity',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['day', 'pool_address', 'token_address'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')],
        post_hook = '{{ expose_spells(\'["sonic"]\',
                                    "project",
                                    "tapio",
                                    \'["brunota20"]\') }}'
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