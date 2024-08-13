{{
    config(
        schema = 'carbon_defi_ethereum',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set weth_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' %}

{{
    carbon_defi_compatible_trades(
        blockchain = 'ethereum',
        project = 'carbon_defi',
        CarbonController_evt_TokensTraded = source('carbon_defi_ethereum', 'CarbonController_evt_TokensTraded'),
        wrapped_native_token = weth_address
    )
}}
