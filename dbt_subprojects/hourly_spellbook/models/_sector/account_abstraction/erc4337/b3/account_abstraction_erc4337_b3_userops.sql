{{ config(
    alias = 'userops',
    schema = 'account_abstraction_erc4337_b3',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['userop_hash', 'tx_hash'],
    post_hook='{{ expose_spells(\'["b3"]\',
                                    "project",
                                    "erc4337",
                                    \'["0xbitfly"]\') }}'
)}}

{% set chain = 'b3' %}
{% set gas_symbol = 'B3' %}
{% set wrapped_gas_address = '0x4200000000000000000000000000000000000006' %}  -- Need to update with actual wrapped gas token address
{% set deployed_date = '2023-02-15' %}

{% set erc4337_models = [
    ref('account_abstraction_erc4337_b3_v0_6_userops_basics'),
    ref('account_abstraction_erc4337_b3_v0_7_userops_basics')
] %}

{{
    erc4337_userops_enrichments(
        blockchain = 'b3',
        base_models = erc4337_models,
        wrapped_gas_address = wrapped_gas_address,
        gas_symbol = gas_symbol,
        deployed_date = deployed_date,
        transactions_model = source('b3', 'transactions'),
        prices_model = source('prices','usd')
    )
}} 