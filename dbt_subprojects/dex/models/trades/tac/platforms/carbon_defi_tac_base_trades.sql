{{
    config(
        schema = 'carbon_defi_tac',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set wtac_address = '0xB63B9f0eb4A6E6f191529D71d4D88cc8900Df2C9' %}

{{
    carbon_defi_compatible_trades(
        blockchain = 'tac',
        project = 'carbon_defi',
        CarbonController_evt_TokensTraded = source('carbon_defi_tac', 'CarbonController_evt_TokensTraded'),
        wrapped_native_token = wtac_address
    )
}}
