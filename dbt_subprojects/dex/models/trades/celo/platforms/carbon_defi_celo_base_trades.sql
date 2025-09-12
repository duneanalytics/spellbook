{{
    config(
        schema = 'carbon_defi_celo',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set wcelo_address = '0x471EcE3750Da237f93B8E339c536989b8978a438' %}
{% set targetToken = '0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE' %}
{% set finalTargetToken = '0x66803FB87aBd4aaC3cbB3fAd7C3aa01f6F3FB207' %}

{{
    carbon_defi_compatible_trades(
        blockchain = 'celo',
        project = 'carbon_defi',
        CarbonController_evt_TokensTraded = source('carbon_defi_celo', 'CarbonController_evt_TokensTraded'),
        CarbonVortex_evt_TokenTraded = source('carbon_defi_celo', 'CarbonVortex_evt_TokenTraded'),
        wrapped_native_token = wcelo_address,
        targetToken = targetToken,
        finalTargetToken = finalTargetToken
    )
}}
