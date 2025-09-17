{{
    config(
        schema = 'carbon_defi_base',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set wbase_address = '0x4200000000000000000000000000000000000006' %}

{{
    carbon_defi_compatible_trades(
        blockchain = 'base',
        project = 'carbon_defi',
        CarbonController_evt_TokensTraded = source('carbon_defi_base', 'CarbonController_evt_TokensTraded'),
        wrapped_native_token = wbase_address
    )
}}
