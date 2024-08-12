{{
    config(
        schema = 'carbon_defi_sei',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set wsei_address = '0xE30feDd158A2e3b13e9badaeABaFc5516e95e8C7' %}

{{
    carbon_defi_compatible_trades(
        blockchain = 'sei',
        project = 'carbon_defi',
        CarbonController_evt_TokensTraded = source('carbon_defi_sei', 'CarbonController_evt_TokensTraded'),
        wrapped_native_token = wsei_address
    )
}}
