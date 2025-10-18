{{ config
(
    alias = 'v0_8_userops_basics',
    schema = 'account_abstraction_erc4337_base',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['userop_hash', 'tx_hash']
)
}}


{% set chain = 'base' %}
{% set gas_symbol = 'ETH' %}
{% set wrapped_gas_address = '0x4200000000000000000000000000000000000006' %}
{% set version = 'v0.8' %}
{% set deployed_date = '2025-03-30' %}

-- macros/models/sector/erc4337
{{
    erc4337_userops_basics(
        blockchain = 'base',
        version = 'v0.8',
        userops_evt_model = source('erc4337_base','entrypoint_evt_useroperationevent'),
        handleops_call_model = source('erc4337_base', 'entrypoint_call_handleops')
    )
}}