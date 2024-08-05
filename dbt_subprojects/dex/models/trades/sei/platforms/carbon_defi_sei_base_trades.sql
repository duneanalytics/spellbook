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

{% set weth_address = '0x160345fc359604fc6e70e3c5facbde5f7a9342d8' %}

{{
    carbon_defi_compatible_trades(
        blockchain = 'sei',
        project = 'carbon_defi',
        CarbonController_evt_TokensTraded = source('carbon_defi_sei', 'CarbonController_evt_TokensTraded'),
        weth_address = weth_address
    )
}}
