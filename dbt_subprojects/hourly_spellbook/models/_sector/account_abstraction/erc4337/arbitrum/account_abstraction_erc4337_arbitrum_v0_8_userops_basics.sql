{{ config
(
    alias = 'v0_8_userops_basics',
    schema = 'account_abstraction_erc4337_arbitrum',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['userop_hash', 'tx_hash']
)
}}


{% set chain = 'arbitrum' %}
{% set gas_symbol = 'ETH' %}
{% set wrapped_gas_address = '0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270' %}
{% set version = 'v0.8' %}
{% set deployed_date = '2025-04-05' %}

-- macros/models/sector/erc4337
{{
    erc4337_userops_basics(
        blockchain = 'arbitrum',
        version = 'v0.8',
        userops_evt_model = source('erc4337_arbitrum','entrypoint_evt_useroperationevent'),
        handleops_call_model = source('erc4337_arbitrum', 'entrypoint_call_handleops')
    )
}}