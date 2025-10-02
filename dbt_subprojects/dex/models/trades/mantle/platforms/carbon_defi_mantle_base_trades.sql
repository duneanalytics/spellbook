{{
    config(
        schema = 'carbon_defi_mantle',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set wmantle_address = '0x78c1b0C915c4FAA5FffA6CAbf0219DA63d7f4cb8' %}

{{
    carbon_defi_compatible_trades(
        blockchain = 'mantle',
        project = 'carbon_defi',
        CarbonController_evt_TokensTraded = source('carbon_defi_mantle', 'CarbonController_evt_TokensTraded'),
        wrapped_native_token = wmantle_address
    )
}}