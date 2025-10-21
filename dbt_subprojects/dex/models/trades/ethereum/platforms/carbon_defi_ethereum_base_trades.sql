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
{% set targetToken = '0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE' %}
{% set finalTargetToken = '0x1F573D6Fb3F13d689FF844B4cE37794d79a7FF1C' %}

{{
    carbon_defi_compatible_trades(
        blockchain = 'ethereum',
        project = 'carbon_defi',
        CarbonController_evt_TokensTraded = source('carbon_defi_ethereum', 'CarbonController_evt_TokensTraded'),
        CarbonVortex_evt_TokenTraded = source('carbon_defi_ethereum', 'CarbonVortex_evt_TokenTraded'),
        wrapped_native_token = weth_address,
        targetToken = targetToken,
        finalTargetToken = finalTargetToken
    )
}}
